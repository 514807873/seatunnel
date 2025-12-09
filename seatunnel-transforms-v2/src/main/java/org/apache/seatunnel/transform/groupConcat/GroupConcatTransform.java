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

package org.apache.seatunnel.transform.groupConcat;

import cn.hutool.json.JSONObject;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.common.GroupConcatQueryResult;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class GroupConcatTransform extends AbstractCatalogSupportTransform {
    private final GroupConcatTransformConfig config;
    private List<String> fieldNames;
    private List<SeaTunnelDataType<?>> fieldTypes;
    private List<Column> outputColumns = new ArrayList<>();

    // SQLite 相关配置
    private String databasePath;
    private String tableName = "seatunnel_data";
    private static boolean driverLoaded = false;

    public GroupConcatTransform(
            @NonNull GroupConcatTransformConfig splitTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        initOutputFields(seaTunnelRowType);
        this.config = splitTransformConfig;
        initSQLiteConfig();
    }

    private void initOutputFields(SeaTunnelRowType inputRowType) {
        this.fieldNames = Arrays.stream(inputRowType.getFieldNames()).collect(Collectors.toList());
        this.fieldTypes = Arrays.stream(inputRowType.getFieldTypes()).collect(Collectors.toList());
    }

    /**
     * 初始化 SQLite 配置（不创建连接）
     */
    private void initSQLiteConfig() {
        // 使用临时文件数据库
        String tempDir = System.getProperty("java.io.tmpdir");
        String uniqueId = UUID.randomUUID().toString();
        databasePath = tempDir + File.separator + "seatunnel_" + uniqueId + ".db";
        log.info("SQLite database path: {}", databasePath);

        // 预创建数据库和表
        createDatabaseAndTable();
    }

    /**
     * 预创建数据库和表
     */
    private void createDatabaseAndTable() {
        Connection connection = null;
        try {
            connection = getConnection();
            createTable(connection);
        } catch (SQLException e) {
            log.error("Failed to create database and table", e);
            throw new RuntimeException("Failed to create database and table", e);
        } finally {
            closeConnection(connection);
        }
    }

    /**
     * 获取数据库连接
     */
    private Connection getConnection() throws SQLException {
        loadSQLiteDriver();
        String url = "jdbc:sqlite:" + databasePath;
        return DriverManager.getConnection(url);
    }

    /**
     * 关闭数据库连接
     */
    private void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Failed to close SQLite connection", e);
            }
        }
    }

    /**
     * 显式加载 SQLite 驱动
     */
    private synchronized void loadSQLiteDriver() {
        if (!driverLoaded) {
            try {
                Class.forName("org.sqlite.JDBC");
                driverLoaded = true;
                log.info("SQLite JDBC driver loaded successfully");
            } catch (ClassNotFoundException e) {
                log.error("Failed to load SQLite JDBC driver", e);
                throw new RuntimeException("Failed to load SQLite JDBC driver", e);
            }
        }
    }

    /**
     * 创建表
     */
    private void createTable(Connection connection) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            SeaTunnelDataType<?> fieldType = fieldTypes.get(i);

            if (i > 0) {
                createTableSQL.append(", ");
            }

            createTableSQL.append(fieldName).append(" ").append(toSQLiteType(fieldType));
        }

        createTableSQL.append(")");

        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL.toString());
            log.info("Table created: {}", createTableSQL);
        }
    }

    /**
     * 将 SeaTunnel 数据类型转换为 SQLite 数据类型
     */
    private String toSQLiteType(SeaTunnelDataType<?> dataType) {
        if (dataType.equals(BasicType.STRING_TYPE)) {
            return "TEXT";
        }
        else if (dataType.equals(BasicType.INT_TYPE)) {
            return "INTEGER";
        }
        else if (dataType.equals(BasicType.LONG_TYPE)) {
            return "INTEGER";
        }
        else if (dataType.equals(BasicType.DOUBLE_TYPE)) {
            return "REAL";
        }
        else if (dataType.equals(BasicType.FLOAT_TYPE)) {
            return "REAL";
        }
        else if (dataType.equals(BasicType.BOOLEAN_TYPE)) {
            return "INTEGER"; // SQLite 没有布尔类型，用 INTEGER 代替
        }
        else {
            return "TEXT"; // 默认使用 TEXT
        }
    }

    /**
     * 将 SeaTunnelRow 数据插入到 SQLite 表中
     */
    private void insertRowToSQLite(SeaTunnelRow row) {
        Connection connection = null;
        try {
            connection = getConnection();
            ensureTableExists(connection);
            StringBuilder insertSQL = new StringBuilder();
            insertSQL.append("INSERT INTO ").append(tableName).append(" (");
            // 构建字段名部分
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i > 0) {
                    insertSQL.append(", ");
                }
                insertSQL.append(fieldNames.get(i));
            }
            insertSQL.append(") VALUES (");
            // 构建占位符部分
            for (int i = 0; i < fieldNames.size(); i++) {
                if (i > 0) {
                    insertSQL.append(", ");
                }
                insertSQL.append("?");
            }
            insertSQL.append(")");
            try (PreparedStatement statement = connection.prepareStatement(insertSQL.toString())) {
                // 设置参数值
                for (int i = 0; i < fieldNames.size(); i++) {
                    Object value = row.getField(i);
                    if (value == null) {
                        statement.setNull(i + 1, Types.NULL);
                    }
                    else {
                        statement.setObject(i + 1, value);
                    }
                }
                statement.executeUpdate();
            }

        } catch (SQLException e) {
            log.error("Failed to insert row into SQLite", e);
            throw new RuntimeException("Failed to insert row into SQLite", e);
        } finally {
            closeConnection(connection);
        }
    }

    private void ensureTableExists(Connection connection) throws SQLException {
        // 检查表是否存在
        boolean tableExists = false;
        try (ResultSet rs = connection.getMetaData().getTables(null, null, tableName, null)) {
            if (rs.next()) {
                tableExists = true;
            }
        }

        // 如果表不存在，创建表
        if (!tableExists) {
            createTable(connection);
        }
    }

    @Override
    public String getPluginName() {
        return "c";
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return null;
    }

    @Override
    public List<SeaTunnelRow> mapList(SeaTunnelRow inputRow) {
        // 将数据写入 SQLite
        insertRowToSQLite(inputRow);
        return Collections.emptyList();
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<String> keyColumn = config.getKeyColumn();
        List<JSONObject> mergeColumn = config.getMergeColumn();
        List<Column> outputColumns = new ArrayList<>();
        for (int i = 0; i < keyColumn.size(); i++) {
            int fieldIndex = fieldNames.indexOf(keyColumn.get(i));
            outputColumns.add(PhysicalColumn.of(fieldNames.get(fieldIndex), fieldTypes.get(fieldIndex), 300, true, ""
                    , ""));
        }
        for (int i = 0; i < mergeColumn.size(); i++) {
            JSONObject entries = mergeColumn.get(i);
            outputColumns.add(PhysicalColumn.of(
                    entries.getStr("newColumn"),
                    BasicType.STRING_TYPE,
                    500,
                    true,
                    "",
                    "")
            );
        }
        this.outputColumns = outputColumns;
        return TableSchema.builder()
                .columns(outputColumns)
                .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public void close() {
        try {
            // 删除数据库文件
            if (databasePath != null) {
                File dbFile = new File(databasePath);
                if (dbFile.exists()) {
                    boolean deleted = dbFile.delete();
                    if (deleted) {
                        log.info("SQLite database file deleted: {}", databasePath);
                    }
                    else {
                        log.warn("Failed to delete SQLite database file: {}", databasePath);
                    }
                }
            }

        } catch (Exception e) {
            log.error("Error while cleaning up SQLite resources", e);
        } finally {
            super.close();
            log.info("GroupConcatTransform closed");
        }
    }

    public GroupConcatQueryResult executeGroupConcatQuery() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            String groupConcatSQL = buildGroupConcatSQL();
            log.info("执行聚合查询: {}", System.lineSeparator() + groupConcatSQL);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(groupConcatSQL);
            // 返回包装对象，资源保持打开状态
            return new GroupConcatQueryResult(connection, statement, resultSet);
        } catch (SQLException e) {
            // 出错时清理资源
            closeResources(resultSet, statement, connection);
            log.error("Failed to execute GROUP_CONCAT query", e);
            throw new RuntimeException("Failed to execute GROUP_CONCAT query", e);
        }
    }

    private void closeResources(ResultSet resultSet, Statement statement, Connection connection) {
        try {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            closeConnection(connection);
        } catch (SQLException e) {
            log.warn("Failed to close resources", e);
        }
    }
    /**
     * 构建 GROUP_CONCAT SQL 查询语句
     */
    private String buildGroupConcatSQL() {
        StringBuilder sql = new StringBuilder();
        // 检查是否需要排序
        boolean needOrdering = false;
        String orderColumn = null;
        if (config.getMergeColumn() != null && !config.getMergeColumn().isEmpty()) {
            cn.hutool.json.JSONObject mergeColumn = config.getMergeColumn().get(0);
            orderColumn = mergeColumn.getStr("orderColumn");
            if (orderColumn != null && !orderColumn.isEmpty()) {
                needOrdering = true;
            }
        }
        if (needOrdering) {
            // 使用子查询来支持排序
            sql.append("SELECT\n");
            // 添加分组字段
            if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                for (int i = 0; i < config.getKeyColumn().size(); i++) {
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    sql.append("    ").append(config.getKeyColumn().get(i));
                }
            }
            // 添加 GROUP_CONCAT 字段
            if (config.getMergeColumn() != null && !config.getMergeColumn().isEmpty()) {
                if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                    sql.append(",\n");
                }
                for (int i = 0; i < config.getMergeColumn().size(); i++) {
                    cn.hutool.json.JSONObject mergeColumn = config.getMergeColumn().get(i);
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    String oldColumn = mergeColumn.getStr("oldColumn");
                    String newColumn = mergeColumn.getStr("newColumn");
                    String separator = mergeColumn.getStr("separator");
                    if (separator == null || separator.isEmpty()) {
                        separator = ",";
                    }
                    sql.append("    GROUP_CONCAT(").append(oldColumn).append(", '").append(separator).append("')");
                    sql.append(" AS ").append(newColumn != null ? newColumn : oldColumn + "_list");
                }
            }

            sql.append("\nFROM (\n");
            sql.append("    SELECT\n");
            // 添加所有字段
            if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                for (int i = 0; i < config.getKeyColumn().size(); i++) {
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    sql.append("        ").append(config.getKeyColumn().get(i));
                }
            }

            // 添加需要聚合的字段
            if (config.getMergeColumn() != null && !config.getMergeColumn().isEmpty()) {
                if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                    sql.append(",\n");
                }
                for (int i = 0; i < config.getMergeColumn().size(); i++) {
                    cn.hutool.json.JSONObject mergeColumn = config.getMergeColumn().get(i);
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    String oldColumn = mergeColumn.getStr("oldColumn");
                    sql.append("        ").append(oldColumn);
                }
            }
            sql.append("\n    FROM\n");
            sql.append("        ").append(tableName);
            sql.append("\n    ORDER BY\n");
            // 添加排序字段
            boolean firstOrder = true;
            if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                for (String keyColumn : config.getKeyColumn()) {
                    if (!firstOrder) {
                        sql.append(",\n");
                    }
                    sql.append("        ").append(keyColumn);
                    firstOrder = false;
                }
            }
            // 添加聚合字段的排序
            if (config.getMergeColumn() != null && !config.getMergeColumn().isEmpty()) {
                for (cn.hutool.json.JSONObject mergeColumn : config.getMergeColumn()) {
                    String mergeOrderColumn = mergeColumn.getStr("orderColumn");
                    if (mergeOrderColumn != null && !mergeOrderColumn.isEmpty()) {
                        if (!firstOrder) {
                            sql.append(",\n");
                        }
                        sql.append("        ").append(mergeOrderColumn);
                        firstOrder = false;
                    }
                }
            }
            sql.append("\n) AS sorted_data\n");
            // 添加 GROUP BY 子句
            if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                sql.append("GROUP BY\n");
                for (int i = 0; i < config.getKeyColumn().size(); i++) {
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    sql.append("    ").append(config.getKeyColumn().get(i));
                }
            }
        }
        else {
            // 不需要排序的简单版本
            sql.append("SELECT\n");
            // 添加分组字段
            if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                for (int i = 0; i < config.getKeyColumn().size(); i++) {
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    sql.append("    ").append(config.getKeyColumn().get(i));
                }
            }
            // 添加 GROUP_CONCAT 字段
            if (config.getMergeColumn() != null && !config.getMergeColumn().isEmpty()) {
                if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                    sql.append(",\n");
                }
                for (int i = 0; i < config.getMergeColumn().size(); i++) {
                    cn.hutool.json.JSONObject mergeColumn = config.getMergeColumn().get(i);
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    String oldColumn = mergeColumn.getStr("oldColumn");
                    String newColumn = mergeColumn.getStr("newColumn");
                    String separator = mergeColumn.getStr("separator");
                    if (separator == null || separator.isEmpty()) {
                        separator = ",";
                    }
                    sql.append("    GROUP_CONCAT(").append(oldColumn).append(", '").append(separator).append("')");
                    sql.append(" AS ").append(newColumn != null ? newColumn : oldColumn + "_list");
                }
            }

            sql.append("\nFROM\n");
            sql.append("    ").append(tableName);
            sql.append("\n");
            // 添加 GROUP BY 子句
            if (config.getKeyColumn() != null && !config.getKeyColumn().isEmpty()) {
                sql.append("GROUP BY\n");
                for (int i = 0; i < config.getKeyColumn().size(); i++) {
                    if (i > 0) {
                        sql.append(",\n");
                    }
                    sql.append("    ").append(config.getKeyColumn().get(i));
                }
            }
        }
        sql.append(";");
        return sql.toString();
    }
}