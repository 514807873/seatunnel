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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.xjjdbc.dialect.XjJdbcDialect;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Pre actions executed before the full-load writing starts. Only the complete (full load) mode is
 * supported.
 */
@Data
@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
public class PreConfig implements Serializable {

    private static final long serialVersionUID = -1L;

    /** Insert mode, only {@code complete} is supported by this connector. */
    private String insertMode = "complete";

    /** Whether to clean (truncate) the target table for the complete mode. */
    private boolean cleanTableWhenComplete = false;

    /**
     * When {@code true}, truncate the target table up front (even if the source produces no rows).
     * When {@code false}, the truncate is deferred to the first incoming row, so an empty source
     * does not wipe the target table.
     */
    private boolean cleanTableWhenCompleteNoDataIn = false;

    /** Pre sql executed when the complete mode does not clean the target table. */
    private String preSql;

    public PreConfig() {}

    public boolean isComplete() {
        return "complete".equalsIgnoreCase(insertMode);
    }

    /** True when the truncate must happen on the first incoming row instead of up front. */
    public boolean isTruncateOnFirstRow() {
        return isComplete() && cleanTableWhenComplete && !cleanTableWhenCompleteNoDataIn;
    }

    /**
     * Run the pre actions once on the coordinator before writers start: up front truncate (when
     * {@code cleanTableWhenCompleteNoDataIn}) or the configured {@code preSql}.
     */
    public void doPreConfig(Connection connection, XjJdbcDialect dialect, XjJdbcSinkConfig config)
            throws SQLException {
        if (!isComplete()) {
            return;
        }
        if (cleanTableWhenComplete && cleanTableWhenCompleteNoDataIn) {
            String truncateSql = dialect.truncateTable(config);
            log.info("XjJdbc preConfig truncate target table: {}", truncateSql);
            try (Statement st = connection.createStatement()) {
                st.execute(truncateSql);
            }
        } else if (!cleanTableWhenComplete && StringUtils.isNotBlank(preSql)) {
            log.info("XjJdbc preConfig execute preSql: {}", preSql);
            try (PreparedStatement ps = connection.prepareStatement(preSql)) {
                ps.execute();
            }
        }
    }
}
