/**
* Sclera - Oracle Connector
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.plugin.dbms.rdbms.oracle.mapper

import com.scleradb.util.tools.Counter

import com.scleradb.objects._

import com.scleradb.sql.objects._
import com.scleradb.sql.datatypes._
import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.mapper._
import com.scleradb.sql.mapper.target._

import com.scleradb.plugin.dbms.rdbms.oracle.location.Oracle

class OracleSqlMapper(loc: Oracle) extends SqlMapper {
    override def queryString(
        query: SqlRelQueryStatement
    ): String = targetQueryString(SqlTranslator.translateQuery(query))

    private def targetQueryString(
        targetQuery: TargetSqlQuery
    ): String = targetQuery match {
        case TargetSqlSelect(distinct, select, from, where,
                             group, having, order, None, None, isAggregate) =>
            val distinctClause: Option[String] = distinct.map {
                case Nil => "DISTINCT"
                case _ =>
                    throw new IllegalArgumentException(
                        "DISTINCT ON is not supported in Oracle"
                    )
            }

            val selectClause: Option[String] =
                Some(select.map(expr => targetExprString(expr)).mkString(", "))

            val fromClause: Option[String] = Some("FROM " + fromString(from))

            val whereClause: Option[String] =
                where.map { pred => "WHERE " + exprString(pred, true) }

            val groupClause: Option[String] = group match {
                case Nil =>
                    if( isAggregate ) Some("GROUP BY NULL") else None
                case exprs =>
                    Some("GROUP BY " +
                         exprs.map(expr => exprString(expr)).mkString(", "))
            }

            val havingClause: Option[String] =
                having.map { pred => "HAVING " + exprString(pred, true) }

            val orderClause: Option[String] = order match {
                case Nil => None
                case exprs =>
                    Some("ORDER BY " +
                         exprs.map(expr => sortExprString(expr)).mkString(", "))
            }

            val clauses: List[Option[String]] = List(
                Some("SELECT"), distinctClause, selectClause,
                fromClause, whereClause, groupClause, havingClause, orderClause
            )

            clauses.flatten.mkString(" ")

        case TargetSqlSelect(distinct, select, from, where,
                             group, having, order, limit, offset, _) =>
            val baseQuery: TargetSqlSelect =
                TargetSqlSelect(
                    distinct, select, from, where, group, having, order
                )

            val baseFrom: TargetSqlFrom =
                TargetSqlNested(Counter.nextSymbol("Q"), baseQuery)

            val offsetExpr: Option[ScalExpr] = offset.map { n =>
                ScalOpExpr(GreaterThan, List(ColRef("ROWNUM"), IntConst(n)))
            }

            val limitExpr: Option[ScalExpr] = limit.map { nLimit =>
                val nOffset: Int = offset getOrElse 0
                val n = nOffset + nLimit

                ScalOpExpr(LessThanEq, List(ColRef("ROWNUM"), IntConst(n)))
            }

            val limitOffsetWhere: Option[ScalExpr] =
                (offsetExpr, limitExpr) match {
                    case (None, exprOpt) => exprOpt
                    case (exprOpt, None) => exprOpt
                    case (Some(lhsExpr), Some(rhsExpr)) =>
                        Some(ScalOpExpr(And, List(lhsExpr, rhsExpr)))
                }

            val rewrite: TargetSqlSelect =
                TargetSqlSelect(from = baseFrom, where = limitOffsetWhere)

            targetQueryString(rewrite)

        case TargetSqlCompound(compoundType, lhs, rhs) =>
            val compoundTypeStr: String = compoundType match {
                case Union => "UNION ALL"
                case Intersect => "INTERSECT"
                case Except => "EXCEPT"
            }

            "(" + targetQueryString(lhs) + ") " + compoundTypeStr +
            " (" + targetQueryString(rhs) + ")"
    }

    private def fromString(from: TargetSqlFrom): String = from match {
        case TargetSqlTableRef(name, Nil) => name

        case TargetSqlTableRef(name, cols) =>
            name + "(" + cols.map(col => exprString(col)).mkString(", ") + ")"

        case TargetSqlValues(name, cols, rows) if( !rows.isEmpty ) =>
            // rewrite needed as Oracle does not allow VALUES in the FROM clause
            val extCols: List[ColRef] =
                cols:::rows.head.scalars.drop(cols.size).map { _ =>
                    ColRef(Counter.nextSymbol("V"))
                }

            val rowQueriesStrs: List[String] = rows.map { row => 
                val targetStrs: List[String] = row.scalars.zip(extCols).map {
                    case (scal, col) => targetExprString(AliasedExpr(scal, col))
                }

                "SELECT " + targetStrs.mkString(", ") +
                " FROM TABLE(sys.odcinumberlist(0))"
            }

            val valuesQueryString: String =
                rowQueriesStrs.tail.foldLeft (rowQueriesStrs.head) {
                    case (prev, qs) => "(" + prev + ") UNION ALL (" + qs + ")"
                }

            "(" + valuesQueryString + ") " + name

        case TargetSqlJoin(Inner, JoinOn(BoolConst(true)),
                           lhsInput, rhsInput) =>

            val lhsStr: String = fromString(lhsInput)
            val rhsStr: String = fromString(rhsInput)

            lhsStr + " CROSS JOIN " + rhsStr

        case TargetSqlJoin(joinType, joinPred, lhsInput, rhsInput) =>
            val joinTypeStr: String = joinType match {
                case Inner => "INNER JOIN"
                case FullOuter => "FULL OUTER JOIN"
                case LeftOuter => "LEFT OUTER JOIN"
                case RightOuter => "RIGHT OUTER JOIN"
            }

            val lhsStr: String = fromString(lhsInput)
            val rhsStr: String = fromString(rhsInput)
            joinPred match {
                case JoinOn(predExpr) =>
                    lhsStr + " " + joinTypeStr + " " + rhsStr + " ON (" +
                        exprString(predExpr, true) +
                    ")"
                case JoinUsing(cols) =>
                    lhsStr + " " + joinTypeStr + " " + rhsStr + " USING (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ")"
                case JoinNatural =>
                    lhsStr + " NATURAL " + joinTypeStr + " " + rhsStr
            }

        case TargetSqlNested(name, query) =>
            "(" + targetQueryString(query) + ") " + name

        case _ =>
            throw new RuntimeException("Cannot map (Oracle): " + from)
    }

    override def updateString(
        stmt: SqlUpdateStatement
    ): List[String] = stmt match {
        case SqlCreateDbObject(obj, duration) =>
            List(createObjectString(obj, duration))

        case SqlDropExplicit(st: SchemaTable, duration) =>
            st.obj.baseType match {
                case Table.BaseTable => 
                    duration match {
                        case Persistent =>
                            List("DROP TABLE " + st.obj.name)
                        case Temporary =>
                            List(
                                "TRUNCATE TABLE " + st.obj.name,
                                "DROP TABLE " + st.obj.name
                            )
                    }

                case Table.BaseView =>
                    List("DROP VIEW " + st.obj.name)
            }

        case SqlCreateIndex(indexName, relationId, indexColRefs, pred) =>
            if( pred == BoolConst(true) )
                List(
                    "CREATE INDEX " + indexName +
                    " ON " + relationId.name + "(" +
                        indexColRefs.map(c => exprString(c)).mkString(", ") +
                    ")"
                )
            else throw new IllegalArgumentException(
                "Partial index not supported in Oracle"
            )

        case SqlDropIndex(indexName) =>
            List("DROP INDEX " + indexName)

        case SqlInsertValueRows(TableId(_, name), targetCols, List(row)) =>
            List(
                "INSERT INTO " +
                fromString(
                    TargetSqlTableRef(name, targetCols)
                ) + " VALUES (" + exprString(row) + ")"
            )

        case SqlInsertQueryResult(tableId, targetCols, Values(_, rows)) =>
            updateString(SqlInsertValueRows(tableId, targetCols, rows))

        case SqlInsertQueryResult(TableId(_, name), targetCols, relExpr) =>
            List(
                "INSERT INTO " +
                fromString(
                    TargetSqlTableRef(name, targetCols)
                ) + " " + queryString(SqlRelQueryStatement(relExpr))
            )

        case SqlUpdate(tableId, cvs, pred) =>
            val setExprs: List[ScalExpr] =
                cvs.map { case (c, v) => ScalOpExpr(Equals, List(c, v)) }
            List(
                "UPDATE " + tableId.name +
                " SET " + setExprs.map(e => exprString(e)).mkString(", ") +
                " WHERE " + exprString(pred, true)
            )

        case SqlDelete(tableId, pred) =>
            List(
                "DELETE FROM " + tableId.name +
                " WHERE " + exprString(pred, true)
            )

        case SqlUpdateBatch(stmts) =>
            stmts.flatMap { stmt => updateString(stmt) }

        case _ =>
            throw new RuntimeException("Cannot map (Oracle): " + stmt)
    }

    private def createObjectString(
        obj: SqlDbObject,
        duration: DbObjectDuration
    ): String = obj match {
        case SqlObjectAsExpr(name, logExpr, DbMaterialized(_)) =>
            duration match {
                case Persistent =>
                    "CREATE TABLE " + name + " AS " +
                    queryString(SqlRelQueryStatement(logExpr))
                case Temporary =>
                    "CREATE GLOBAL TEMPORARY TABLE " + name +
                    " ON COMMIT PRESERVE ROWS AS " +
                    queryString(SqlRelQueryStatement(logExpr))
            }

        case SqlObjectAsExpr(name, logExpr, DbVirtual) =>
            duration match {
                case Persistent =>
                    "CREATE VIEW " + name + " AS " +
                    queryString(SqlRelQueryStatement(logExpr))
                case Temporary =>
                    throw new IllegalArgumentException(
                        "TEMPORARY VIEW is not supported in Oracle"
                    )
            }

        case SqlTable(table, _, relExprOpt) =>
            val pkCols: List[String] = table.keyOpt match {
                case Some(PrimaryKey(cols)) => cols.map { col => col.name }
                case None => Nil
            }

            val fkCols: List[String] = table.foreignKeys.flatMap {
                case ForeignKey(cols, _, _, _) => cols.map { col => col.name }
            }

            val indexCols: List[String] = pkCols:::fkCols

            val colStrs: List[String] = table.columns.map {
                case Column(colName, colType, None) =>
                    val colTypeStr: String =
                        if( indexCols contains colName ) {
                            // the type needs to be index-friendly
                            colType match {
                                case SqlOption(t) => sqlTypeIndexString(t)
                                case t => sqlTypeIndexString(t) + " NOT NULL"
                            }
                        } else {
                            colType match {
                                case SqlOption(t) => sqlTypeString(t)
                                case t => sqlTypeString(t) + " NOT NULL"
                            }
                        }

                    colName + " " + colTypeStr
                case _ =>
                    throw new RuntimeException("Column families not handled")
            }

            val keyStrs: List[String] = table.keyOpt.toList.map {
                case PrimaryKey(cols) =>
                    "PRIMARY KEY (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ")"
            }

            val refStrs: List[String] =
                table.foreignKeys.filter { fk =>
                    fk.refTableId(loc.schema).locationId == loc.id
                } map { case ForeignKey(cols, _, refTableName, refCols) =>
                    "FOREIGN KEY (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ") REFERENCES " +
                    fromString(TargetSqlTableRef(refTableName, refCols))
                }


            val createStr: String = duration match {
                case Persistent =>
                    "CREATE TABLE " + table.name +
                    "(" + (colStrs:::keyStrs:::refStrs).mkString(", ") + ")"
                case Temporary =>
                    "CREATE GLOBAL TEMPORARY TABLE " + table.name +
                    "(" + (colStrs:::keyStrs:::refStrs).mkString(", ") + ")" +
                    " ON COMMIT PRESERVE ROWS"
            }

            relExprOpt match {
                case Some(relExpr) =>
                    createStr + " AS " +
                    queryString(SqlRelQueryStatement(relExpr))
                case None =>
                    createStr
            }

        case _ =>
            throw new RuntimeException("Cannot map (Oracle): " + obj)
    }

    private def exprString(
        scalExpr: ScalExpr,
        isPredicate: Boolean = false
    ): String = scalExpr match {
        case ColRef(name) =>
            if( isPredicate) { name + " = 1" } else name

        case AnnotColRef(Some(tName), cName) => tName + "." + cName

        case AnnotColRef(None, cName) => cName

        case IntConst(value) => value.toString

        case ShortConst(value) => value.toString

        case LongConst(value) => value.toString

        case FloatConst(value) => value.toString

        case DoubleConst(value) => value.toString

        case BoolConst(value) =>
            if( isPredicate ) {
                if( value ) "1 = 1" else "0 = 1"
            } else {
                if( value ) "1" else "0"
            }

        case CharConst(value) =>
            "'" + value.replaceAll("'", "''") + "'"

        case DateConst(value) => "DATE '" + value.toString + "'"

        case TimestampConst(value) => "TIMESTAMP '" + value.toString + "'"

        case TimeConst(value) =>
            val ts: java.sql.Timestamp = new java.sql.Timestamp(value.getTime)
            "TIMESTAMP '" + ts.toString + "'"

        case SqlTypedNull(t) => "CAST(NULL AS " + sqlTypeCastString(t) + ")"

        case Pattern(pat, esc) => pat + " ESCAPE " + esc

        case Row(vs) => vs.map(s => exprString(s)).mkString(", ")

        case ScalSubQuery(relExpr) => queryString(SqlRelQueryStatement(relExpr))

        case Exists(relExpr) =>
            "EXISTS (" + queryString(SqlRelQueryStatement(relExpr)) + ")"

        case ScalOpExpr(cmpOp: ScalRelCmpOp,
                        List(lhs, ScalCmpRelExpr(qual, subQueryOrList))) =>
            val cmpQualStr: String = qual match {
                case CmpAll => "ALL"
                case CmpAny => "ANY"
            }

            "(" + exprString(lhs) + ") " +
            cmpOpString(cmpOp) + " " + cmpQualStr +
            " (" + relSubQueryString(subQueryOrList) + ")"

        case ScalOpExpr(TypeCast(t), List(input)) =>
            "CAST(" + exprString(input) + " AS " + sqlTypeCastString(t) + ")"

        case ScalOpExpr(UnaryPlus, List(input)) =>
            "+(" + exprString(input) + ")"

        case ScalOpExpr(UnaryMinus, List(input)) =>
            "-(" + exprString(input) + ")"

        case ScalOpExpr(Plus, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") + (" + exprString(rhs) + ")"

        case ScalOpExpr(Minus, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") - (" + exprString(rhs) + ")"

        case ScalOpExpr(Mult, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") * (" + exprString(rhs) + ")"

        case ScalOpExpr(Div, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") / (" + exprString(rhs) + ")"

        case ScalOpExpr(Modulo, List(lhs, rhs)) =>
            "MOD(" + exprString(lhs) + ", " + exprString(rhs) + ")"

        case ScalOpExpr(Exp, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") ^ (" + exprString(rhs) + ")"

        case ScalOpExpr(Not, List(input)) =>
            val predString: String = exprString(input, true)
            if( isPredicate ) {
                "NOT (" + predString + ")"
            } else {
                "CASE WHEN (" + predString + ") THEN 0 ELSE 1 END"
            }

        case ScalOpExpr(Or, List(lhs, rhs)) =>
            val predString: String =
                "(" + exprString(lhs, true) +
                ") OR (" + exprString(rhs, true) + ")"
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(And, List(lhs, rhs)) =>
            val predString: String =
                "(" + exprString(lhs, true) +
                ") AND (" + exprString(rhs, true) + ")"
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(IsNull, List(input)) =>
            val predString: String = "(" + exprString(input) + ") IS NULL"
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(IsLike(pat), List(lhs)) =>
            val predString: String =
                "(" + exprString(lhs) + ") LIKE " + exprString(pat)
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(IsILike(pat), List(lhs)) =>
            val predString: String =
                "(" + exprString(lhs) + ") ILIKE " + exprString(pat)
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(IsSimilarTo(pat), List(lhs)) =>
            val predString: String =
                "(" + exprString(lhs) + ") SIMILAR TO " + exprString(pat)
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(IsDistinctFrom, List(lhs, rhs)) =>
            val predString: String =
                "(" + exprString(lhs) +
                ") IS DISTINCT FROM (" + exprString(rhs) + ")"
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(IsBetween(qual), List(expr, lhs, rhs)) =>
            val rangeQualStr: String = qual match {
                case Symmetric => "SYMMETRIC"
                case Asymmetric => ""
            }

            val predString: String =
                "(" + exprString(expr) + ") IS BETWEEN " + rangeQualStr +
                " (" + exprString(lhs) + ") AND (" + exprString(rhs) + ")"
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(cmpOp: ScalRelCmpOp, List(lhs, rhs)) =>
            val predString: String =
                "(" + exprString(lhs) + ") " + cmpOpString(cmpOp) +
                " (" + exprString(rhs) + ")"
            if( isPredicate ) predString else {
                "CASE WHEN (" + predString + ") THEN 1 ELSE 0 END"
            }

        case ScalOpExpr(AggregateFunction(name, qual), inputs) =>
            val funcQualStr: String = qual match {
                case FuncAll => ""
                case FuncDistinct => "DISTINCT "
            }

            functionAlias(name) + "(" +
                funcQualStr + inputs.map(e => exprString(e)).mkString(", ") +
            ")"

        case ScalOpExpr(ScalarFunction("CURRENT_DATE"), Nil) =>
            "CURRENT_DATE"
        case ScalOpExpr(ScalarFunction("CURRENT_TIME"), Nil) =>
            "CURRENT_TIMESTAMP"
        case ScalOpExpr(ScalarFunction("CURRENT_TIMESTAMP"), Nil) =>
            "CURRENT_TIMESTAMP"

        case ScalOpExpr(ScalarFunction("DATE_PART"), List(spec, d)) =>
            spec match {
                case CharConst(evalSpec)
                if evalSpec.toUpperCase == "DAY_OF_WEEK" =>
                    "TO_CHAR(" + exprString(d) + ", 'D')"
                case CharConst(evalSpec)
                if evalSpec.toUpperCase == "DAY_OF_YEAR" =>
                    "TO_CHAR(" + exprString(d) + ", 'DDD')"
                case CharConst(evalSpec) =>
                    "EXTRACT(" + evalSpec + " FROM " + exprString(d) + ")"
                case _ =>
                    throw new IllegalArgumentException(
                        "Cannot compute DATE_PART on " +
                        "unknown specification \"" + spec.repr + "\""
                    )
            }

        case ScalOpExpr(ScalarFunction(name), inputs) =>
            functionAlias(name) +
            "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val isSwitch: Boolean = argExpr match {
                case BoolConst(true) => false
                case _ => true
            }

            val caseArgStrs: List[String] =
                if( isSwitch ) List("CASE", exprString(argExpr))
                else List("CASE")

            val whenThenStrs: List[String] =
                whenThen.flatMap {
                    case (w, t) => List(
                        "WHEN", exprString(w, !isSwitch),
                        "THEN", exprString(t)
                    )
                }

            val defaultStrs: List[String] = defaultExpr match {
                case (_: SqlNull) => Nil
                case expr => List("ELSE", exprString(expr))
            }

            val endStrs: List[String] = List("END")
            List(
                caseArgStrs, whenThenStrs, defaultStrs, endStrs
            ).flatten.mkString(" ")

        case _ => throw new RuntimeException("Cannot map (Oracle): " + scalExpr)
    }

    private def cmpOpString(cmpOp: ScalRelCmpOp): String = cmpOp match {
        case Equals => "="
        case NotEquals => "<>"
        case LessThan => "<"
        case LessThanEq => "<="
        case GreaterThan => ">"
        case GreaterThanEq => ">="
    }

    private def targetExprString(
        targetExpr: TargetExpr
    ): String = targetExpr match {
        case StarTargetExpr(Some((tname, None)), Nil) => tname + ".*"
        case StarTargetExpr(None, Nil) => "*"
        case (t: ScalarTarget) =>
            "(" + exprString(t.expr) + ") AS " + exprString(t.alias)
        case _ =>
            throw new RuntimeException("Cannot map (Oracle): " + targetExpr)
    }

    private def sortExprString(sortExpr: SortExpr): String = sortExpr match {
        case SortExpr(expr, sortDir, nullsOrder) =>
            val sortDirStr: String = sortDir match {
                case SortAsc => "ASC"
                case SortDesc => "DESC"
            }

            val nullsOrderStr: String = nullsOrder match {
                case NullsFirst => "NULLS FIRST"
                case NullsLast => "NULLS LAST"
            }

            "(" + exprString(expr) + ") " + sortDirStr + " " + nullsOrderStr
    }

    private def relSubQueryString(
        subQueryOrList: RelSubQueryBase
    ): String = subQueryOrList match {
        case ScalarList(exprs) => exprs.map(e => exprString(e)).mkString(", ")
        case RelSubQuery(relExpr) => queryString(SqlRelQueryStatement(relExpr))
    }

    private def sqlTypeString(sqlType: SqlType): String = sqlType match {
        case SqlInteger => "NUMBER(10)"
        case SqlSmallInt => "NUMBER(5)"
        case SqlBigInt => "NUMBER(38)"
        case SqlDecimal(None, None) => "FLOAT"
        case SqlDecimal(Some(p), None) => "FLOAT(" + p + ")"
        case SqlDecimal(Some(p), Some(s)) => "NUMBER(" + p + ", " + s + ")"
        case SqlFloat(None) => "FLOAT"
        case SqlFloat(Some(p)) => "FLOAT(" + p + ")"
        case SqlReal => "REAL"
        case SqlBool => "NUMBER(1)"
        case SqlCharFixed(None) => "CHAR"
        case SqlCharFixed(Some(l)) => "CHAR(" + l + ")"
        case (SqlCharVarying(None) | SqlText) =>
            "VARCHAR(" + SqlType.maxVarCharLen + ")"
        case SqlCharVarying(Some(l)) => "VARCHAR(" + l + ")"
        case SqlTimestamp => "TIMESTAMP"
        case SqlTime => "TIMESTAMP"
        case SqlDate => "DATE"
        case SqlOption(baseType) => sqlTypeString(baseType)
        case _ => throw new RuntimeException("Cannot map (Oracle): " + sqlType)
    }

    private def sqlTypeCastString(
        sqlType: SqlType
    ): String = sqlTypeString(sqlType)

    private def sqlTypeIndexString(
        sqlType: SqlType
    ): String = sqlTypeString(sqlType)

    override val functionMapOpt: Option[Map[String, String]] = Some(
        Map(
            "ABS" -> "ABS",
            "AVG" -> "AVG",
            "CEIL" -> "CEIL",
            "CEILING" -> "CEIL",
            "CHARACTER_LENGTH" -> "LENGTH",
            "CHAR_LENGTH" -> "LENGTH",
            "COALESCE" -> "COALESCE",
            "CONCAT" -> "CONCAT",
            "CORR" -> "CORR",
            "COUNT" -> "COUNT",
            "COVAR_POP" -> "COVAR_POP",
            "COVAR_SAMP" -> "COVAR_SAMP",
            "CURRENT_DATE" -> "CURRENT_DATE",
            "CURRENT_TIME" -> "CURRENT_TIME",
            "CURRENT_TIMESTAMP" -> "CURRENT_TIMESTAMP",
            "DATE_PART" -> "DATE_PART",
            "DENSE_RANK" -> "DENSE_RANK",
            "EXP" -> "EXP",
            "FLOOR" -> "FLOOR",
            "GREATEST" -> "GREATEST",
            "LEAST" -> "LEAST",
            "LN" -> "LN",
            "LOG" -> "LOG",
            "LOWER" -> "LOWER",
            "MAX" -> "MAX",
            "MIN" -> "MIN",
            "MOD" -> "MOD",
            "NTH_VALUE" -> "NTH_VALUE",
            "NULLIF" -> "NULLIF",
            "POWER" -> "POWER",
            "RANK" -> "RANK",
            "REGR_AVGX" -> "REGR_AVGX",
            "REGR_AVGY" -> "REGR_AVGY",
            "REGR_COUNT" -> "REGR_COUNT",
            "REGR_INTERCEPT" -> "REGR_INTERCEPT",
            "REGR_R2" -> "REGR_R2",
            "REGR_SLOPE" -> "REGR_SLOPE",
            "REGR_SXX" -> "REGR_SXX",
            "REGR_SXY" -> "REGR_SXY",
            "REGR_SYY" -> "REGR_SYY",
            "REPLACE" -> "REPLACE",
            "ROUND" -> "ROUND",
            "ROW_NUMBER" -> "ROW_NUMBER",
            "SIGN" -> "SIGN",
            "SQRT" -> "SQRT",
            "STDDEV" -> "STDDEV",
            "STDDEV_POP" -> "STDDEV_POP",
            "STDDEV_SAMP" -> "STDDEV_SAMP",
            "STRPOS" -> "INSTR",
            "SUBSTRING" -> "SUBSTRING",
            "SUM" -> "SUM",
            "TRIM" -> "TRIM",
            "TRUNC" -> "TRUNC",
            "TRUNCATE" -> "TRUNC",
            "UPPER" -> "UPPER",
            "VARIANCE" -> "VARIANCE",
            "VAR_POP" -> "VAR_POP",
            "VAR_SAMP" -> "VAR_SAMP",
            "ASIN" -> "ASIN",
            "ACOS" -> "ACOS",
            "ATAN" -> "ATAN",
            "ATAN2" -> "ATAN2",
            "SIN" -> "SIN",
            "COS" -> "COS",
            "TAN" -> "TAN"
        )
    )
}
