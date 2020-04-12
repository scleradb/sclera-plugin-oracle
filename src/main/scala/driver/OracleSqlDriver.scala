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

import com.scleradb.dbms.location.Location
import com.scleradb.sql.mapper.SqlMapper
import com.scleradb.dbms.rdbms.driver.StdSqlDriver

class OracleSqlDriver(
    override val location: Location,
    override val sqlMapper: SqlMapper,
    override val jdbcUrl: String,
    override val configProps: List[(String, String)]
) extends StdSqlDriver {
    override val metadataDriver: OracleSqlMetadataDriver =
        new OracleSqlMetadataDriver(this, conn.getMetaData())
}
