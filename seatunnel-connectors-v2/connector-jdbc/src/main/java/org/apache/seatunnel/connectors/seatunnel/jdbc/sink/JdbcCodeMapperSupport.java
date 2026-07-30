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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
final class JdbcCodeMapperSupport {

    private JdbcCodeMapperSupport() {}

    static CodeConverter createCodeConverter(JdbcSinkConfig jdbcSinkConfig) {
        CodeConverter converter = new CodeConverter();
        Map<String, String> codeMapper = jdbcSinkConfig.getCodeMapper();
        if (codeMapper == null || codeMapper.isEmpty()) {
            return converter;
        }
        List<String> dmCodes =
                codeMapper.values().stream()
                        .filter(value -> value.startsWith("DM"))
                        .distinct()
                        .collect(Collectors.toList());
        if (dmCodes.isEmpty()) {
            return converter;
        }
        Map<String, String> dmMap = loadDmMap(dmCodes);
        converter.setDmMap(dmMap);
        return converter;
    }

    private static Map<String, String> loadDmMap(List<String> dmCodes) {
        Map<String, String> dmMap = new HashMap<>();
        String url = System.getenv("PANGU_MYSQL_URL");
        String user = System.getenv("PANGU_MYSQL_ROOT_USER");
        String password = System.getenv("PANGU_MYSQL_ROOT_PASSWORD");
        if (url == null || user == null || password == null) {
            log.warn(
                    "code_mapper contains DM mappings but PANGU mysql env is missing, skip preload");
            return dmMap;
        }
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        try (Connection connection = DriverManager.getConnection(url, info);
                Statement statement = connection.createStatement()) {
            for (String dmCode : dmCodes) {
                String[] split = dmCode.split("\\.");
                if (split.length < 4) {
                    continue;
                }
                String sql = String.format("select %s,%s from %s", split[2], split[3], split[1]);
                try (ResultSet rs = statement.executeQuery(sql)) {
                    while (rs.next()) {
                        dmMap.put(dmCode + "." + rs.getString(split[2]), rs.getString(split[3]));
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to preload DM code mappings from pangu mysql", e);
        }
        return dmMap;
    }
}
