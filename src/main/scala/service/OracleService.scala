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

package com.scleradb.plugin.dbms.rdbms.oracle.service

import com.scleradb.exec.Schema

import com.scleradb.dbms.location.{LocationId, LocationPermit}
import com.scleradb.dbms.service.DBService

import com.scleradb.plugin.dbms.rdbms.oracle.location.Oracle

class OracleService extends DBService {
    override val id: String = Oracle.id

    override def createLocation(
        schema: Schema,
        locationId: LocationId,
        paramDbName: String,
        dbSchemaOpt: Option[String],
        config: List[(String, String)],
        permit: LocationPermit
    ): Oracle = {
        dbSchemaOpt.foreach { s =>
            throw new IllegalArgumentException(
                "Oracle does not support multiple schemas for a user"
            )
        }

        new Oracle(schema, locationId, paramDbName, config, permit)
    }
}
