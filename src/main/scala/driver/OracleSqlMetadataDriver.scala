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

package com.scleradb.plugin.dbms.rdbms.oracle.driver

import java.sql.DatabaseMetaData

import com.scleradb.sql.result.TableResult
import com.scleradb.dbms.rdbms.driver.{SqlDriver, SqlMetadataDriver}

private[scleradb]
class OracleSqlMetadataDriver(
    driver: SqlDriver,
    metaData: DatabaseMetaData
) extends SqlMetadataDriver(driver, metaData) {
    private val tableQuery: Map[String, String] = Map(
        "TABLE" -> (
            "SELECT TABLE_NAME AS TABLE_NAME, 'TABLE' AS TABLE_TYPE" +
            " FROM USER_TABLES WHERE TABLESPACE_NAME IS NOT NULL"
        ),
        "TEMPORARY TABLE" -> (
            "SELECT TABLE_NAME AS TABLE_NAME, 'TEMPORARY TABLE' AS TABLE_TYPE" +
            " FROM USER_TABLES WHERE TABLESPACE_NAME IS NULL"
        ),
        "VIEW" -> (
            "SELECT VIEW_NAME AS TABLE_NAME, 'VIEW' AS TABLE_TYPE" +
            " FROM USER_VIEWS"
        ),
        "MATERIALIZED VIEW" -> (
            "SELECT MVIEW_NAME AS TABLE_NAME," +
            " 'MATERIALIZED VIEW' AS TABLE_TYPE FROM USER_MVIEWS"
        )
    )

    override def tablesMetadata(
        catalogOpt: Option[String], // catalogs not suported in Oracle, ignored
        schemaOpt: Option[String], // schemas not suported in Oracle, ignored
        tableNameOpt: Option[String],
        tableTypeStrsOpt: Option[List[String]]
    ): List[(String, String)] = {
        val tableTypeStrs: List[String] =
            tableTypeStrsOpt getOrElse tableQuery.keys.toList
        val queryStrs: List[String] =
            tableTypeStrs.flatMap { s => tableQuery.get(s.toUpperCase) }
        val baseQueryStr: String = queryStrs mkString " UNION ALL "
        val queryStr: String = tableNameOpt match {
            case Some(tname) =>
                val wrapper: String =
                    "SELECT * FROM (%s) X WHERE UPPER(TABLE_NAME) = '%s'"
                wrapper.format(baseQueryStr, tname.toUpperCase)
            case None => baseQueryStr
        }

        val result: TableResult = driver.executeQuery(queryStr)

        try {
            result.rows.toList.map { row =>
                val tableName: String = row.getStringOpt("TABLE_NAME").get
                val tableType: String = row.getStringOpt("TABLE_TYPE").get

                (tableName, tableType)
            }
        } finally {
            result.close()
        }
    }
}
