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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.converter;

import org.apache.seatunnel.api.table.type.SqlType;

import lombok.Data;

import java.util.function.Supplier;

/**
 * One resolved source-to-sink column mapping.
 *
 * <p>Reading a value uses {@link #sourceRowPosition} (the position of the source field inside the
 * upstream {@code SeaTunnelRow}); writing uses {@link #sinkColumnName}. Source and sink may differ
 * in name, order and count, which is the whole point of a non 1:1 mapping.
 */
@Data
public class ColumnMapper {

    /** Source field name (the key of {@code field_mapper}). */
    private String sourceColumnName;

    /** Position of the source field inside the upstream row type. */
    private int sourceRowPosition;

    /** SeaTunnel sql type of the source field, used as an auxiliary hint when binding. */
    private SqlType sourceSqlType;

    /** Sink column name (the value of {@code field_mapper}). */
    private String sinkColumnName;

    /** The real database column type name of the sink column, read from table metadata. */
    private String sinkColumnDbType;

    /** Placeholder supplier for the insert statement, always {@code ?} for the full load. */
    private Supplier<String> valueSupplier = () -> "?";
}
