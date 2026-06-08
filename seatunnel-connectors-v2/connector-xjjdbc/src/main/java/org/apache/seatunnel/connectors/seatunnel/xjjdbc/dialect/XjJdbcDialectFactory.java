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

/** Resolve the {@link XjJdbcDialect} by the configured {@code db_type}. */
public final class XjJdbcDialectFactory {

    private XjJdbcDialectFactory() {}

    public static XjJdbcDialect getJdbcDialect(String dbType) {
        if (dbType == null) {
            throw new IllegalArgumentException("db_type must not be null");
        }
        switch (dbType.toLowerCase()) {
            case "mysql":
            case "mysql5":
            case "mariadb":
                return new MysqlDialect();
            case "oracle":
                return new OracleDialect();
            case "pgsql":
            case "postgres":
            case "postgresql":
            case "hexadb":
                return new PostgresDialect();
            case "sqlserver":
            case "sqlserver2000":
                return new SqlServerDialect();
            case "clickhouse":
                return new ClickHouseDialect();
            case "dameng":
                return new DaMengDialect();
            case "trino":
                return new TrinoDialect();
            default:
                throw new IllegalArgumentException("Unsupported db_type: " + dbType);
        }
    }
}
