/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect;

import org.apache.seatunnel.connectors.seatunnel.xjjdbc.config.XjJdbcSinkConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Trino dialect (no JDBC transactions, truncate emulated by delete). */
public class TrinoDialect implements XjJdbcDialect {

    @Override
    public String dialectName() {
        return "Trino";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public boolean useAutoCommit() {
        return true;
    }

    @Override
    public void initConnection(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION hive.insert_existing_partitions_behavior = 'APPEND'");
        }
    }

    @Override
    public String truncateTable(XjJdbcSinkConfig config) {
        return "delete from " + tableWithSchema(config);
    }
}
