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

package com.scleradb.plugin.dbms.rdbms.oracle.location

import com.scleradb.exec.Schema
import com.scleradb.dbms.location.{LocationId, LocationPermit}
import com.scleradb.sql.mapper.SqlMapper

import com.scleradb.plugin.dbms.rdbms.oracle.mapper.OracleSqlMapper
import com.scleradb.plugin.dbms.rdbms.oracle.driver.OracleSqlDriver

import com.scleradb.dbms.rdbms.location.RdbmsLocation

class Oracle(
    override val schema: Schema,
    override val id: LocationId,
    override val dbName: String,
    baseConfig: List[(String, String)],
    override val permit: LocationPermit
) extends RdbmsLocation {
    override val isTemporary: Boolean = false
    override val dbms: String = Oracle.id
    override val param: String =
        if( dbName contains "/" ) dbName else "localhost:1521/" + dbName
    override val dbSchemaOpt: Option[String] = None

    override val sqlMapper: SqlMapper = new OracleSqlMapper(this)
    override val url: String = "jdbc:oracle:thin:@//" + param
    override val config: List[(String, String)] =
        (("oracle.net.CONNECT_TIMEOUT", "3000")::baseConfig).distinct

    override def driver: OracleSqlDriver =
        new OracleSqlDriver(this, sqlMapper, url, config)
}

object Oracle {
    val id: String = "ORACLE"
}
