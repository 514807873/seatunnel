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

package com.qh.sqlcdc.compare;

import java.util.List;
import java.util.Locale;

/**
 * 将原始查询包成带 ORDER BY 的子查询，供双游标归并使用。
 */
public final class OrderedQueryBuilder {

    private OrderedQueryBuilder() {}

    public static String wrapOrderBy(String query, List<String> orderColumns, String dbType) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("query 不能为空");
        }
        if (orderColumns == null || orderColumns.isEmpty()) {
            throw new IllegalArgumentException("ORDER BY 列不能为空");
        }
        String trimmed = query.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        StringBuilder orderBy = new StringBuilder();
        for (String col : orderColumns) {
            if (orderBy.length() > 0) {
                orderBy.append(", ");
            }
            orderBy.append(quoteIdentifier(col, dbType));
        }
        return "SELECT * FROM (" + trimmed + ") q ORDER BY " + orderBy;
    }

    public static String quoteIdentifier(String identifier, String dbType) {
        if (identifier == null) {
            return null;
        }
        String trimmed = identifier.trim();
        if (isAlreadyQuoted(trimmed)) {
            return trimmed;
        }
        String type = dbType == null ? "" : dbType.trim().toLowerCase(Locale.ROOT);
        if (type.contains("mysql") || type.contains("mariadb") || type.contains("clickhouse") || type.contains("starrocks")) {
            return "`" + trimmed.replace("`", "``") + "`";
        }
        if (type.contains("sqlserver") || type.contains("mssql")) {
            return "[" + trimmed.replace("]", "]]") + "]";
        }
        // oracle / postgresql / 默认
        return "\"" + trimmed.replace("\"", "\"\"") + "\"";
    }

    private static boolean isAlreadyQuoted(String identifier) {
        return (identifier.startsWith("`") && identifier.endsWith("`"))
                || (identifier.startsWith("\"") && identifier.endsWith("\""))
                || (identifier.startsWith("[") && identifier.endsWith("]"));
    }
}
