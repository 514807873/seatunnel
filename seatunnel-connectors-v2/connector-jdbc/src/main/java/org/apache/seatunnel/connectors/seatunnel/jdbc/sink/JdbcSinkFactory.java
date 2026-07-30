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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.sink.SinkCommonOptions.MULTI_TABLE_SINK_REPLICA;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY;
import static org.apache.seatunnel.api.sink.SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.ALL_Columns;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.AUTO_COMMIT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.COMPATIBLE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.CUSTOM_SQL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DATA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.DRIVER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.FIELD_MAPPER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.GENERATE_SINK_SQL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.IS_EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.MAX_COMMIT_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.SCHEMA_SAVE_MODE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.TRANSACTION_TIMEOUT_SEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.USER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.XA_DATA_SOURCE_CLASS_NAME;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions.recordOperation;

@AutoService(Factory.class)
public class JdbcSinkFactory implements TableSinkFactory {

    private static final String OPERATE_FLAG_COLUMN = "operateflag";
    private static final String OPERATE_TIME_COLUMN = "operatetime";

    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    private ReadonlyConfig getCatalogOptions(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        // TODO Remove obsolete code
        Optional<Map<String, String>> catalogOptions =
                config.getOptional(CatalogOptions.CATALOG_OPTIONS);
        if (catalogOptions.isPresent()) {
            return ReadonlyConfig.fromMap(new HashMap<>(catalogOptions.get()));
        }
        return config;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        Optional<Map<String, String>> optionalFieldMapper = config.getOptional(FIELD_MAPPER);

        List<Column> newColumns = new ArrayList<>();
        List<String> newPrimaryKeyColumnNames = new ArrayList<>();
        if (optionalFieldMapper.isPresent()) {
            Map<String, String> fieldMapper = optionalFieldMapper.get();

            for (Column column : catalogTable.getTableSchema().getColumns()) {
                String oldName = column.getName();
                String newName = fieldMapper.get(oldName);
                if (newName != null) {
                    Column newColumn = column.rename(newName);
                    newColumns.add(newColumn);
                }
            }

            if (null != primaryKey) {
                List<String> columnNames = primaryKey.getColumnNames();
                for (String columnName : columnNames) {
                    newPrimaryKeyColumnNames.add(fieldMapper.get(columnName));
                }
            }
        }
        ReadonlyConfig catalogOptions = getCatalogOptions(context);
        Optional<String> optionalTable = config.getOptional(TABLE);
        Optional<String> optionalDatabase = config.getOptional(DATABASE);
        if (!optionalTable.isPresent()) {
            optionalTable = Optional.of(REPLACE_TABLE_NAME_KEY);
        }
        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName = tableId.getDatabaseName();
        String sourceSchemaName = tableId.getSchemaName();
        String sourceTableName = tableId.getTableName();
        // get sink table relevant information
        String sinkDatabaseName = optionalDatabase.orElse(REPLACE_DATABASE_NAME_KEY);
        String sinkTableNameBefore = optionalTable.get();
        String[] sinkTableSplitArray = sinkTableNameBefore.split("\\.");
        String sinkTableName = sinkTableSplitArray[sinkTableSplitArray.length - 1];
        String sinkSchemaName;
        if (sinkTableSplitArray.length > 1) {
            sinkSchemaName = sinkTableSplitArray[sinkTableSplitArray.length - 2];
        }
        else {
            sinkSchemaName = null;
        }
        if (StringUtils.isNotBlank(catalogOptions.get(JdbcCatalogOptions.SCHEMA))) {
            sinkSchemaName = catalogOptions.get(JdbcCatalogOptions.SCHEMA);
        }
        // to add tablePrefix and tableSuffix
        String tempTableName;
        String prefix = catalogOptions.get(JdbcCatalogOptions.TABLE_PREFIX);
        String suffix = catalogOptions.get(JdbcCatalogOptions.TABLE_SUFFIX);
        if (StringUtils.isNotEmpty(prefix) || StringUtils.isNotEmpty(suffix)) {
            tempTableName = StringUtils.isNotEmpty(prefix) ? prefix + sinkTableName : sinkTableName;
            tempTableName = StringUtils.isNotEmpty(suffix) ? tempTableName + suffix : tempTableName;

        }
        else {
            tempTableName = sinkTableName;
        }
        // to replace
        String finalDatabaseName = sinkDatabaseName;
        if (StringUtils.isNotEmpty(sourceDatabaseName)) {
            finalDatabaseName =
                    sinkDatabaseName.replace(REPLACE_DATABASE_NAME_KEY, sourceDatabaseName);
        }

        String finalSchemaName;
        if (sinkSchemaName != null) {
            if (sourceSchemaName == null) {
                finalSchemaName = sinkSchemaName;
            }
            else {
                finalSchemaName = sinkSchemaName.replace(REPLACE_SCHEMA_NAME_KEY, sourceSchemaName);
            }
        }
        else {
            finalSchemaName = null;
        }
        String finalTableName = sinkTableName;
        if (StringUtils.isNotEmpty(sourceTableName)) {
            finalTableName = tempTableName.replace(REPLACE_TABLE_NAME_KEY, sourceTableName);
        }

        // recordOperation=true 时，从目标库读取 operateflag/operatetime 的真实列名
        // （忽略大小写匹配），保证生成 SQL 的列名大小写与数据库一致；缺列直接报错
        if (optionalFieldMapper.isPresent()
                && config.getOptional(recordOperation).orElse(false)) {
            appendOperationColumns(
                    newColumns,
                    JdbcConnectionConfig.of(config),
                    finalDatabaseName,
                    finalSchemaName,
                    finalTableName);
        }

        TableSchema newtableSchema;
        if (primaryKey != null) {
            PrimaryKey newPrimaryKey = PrimaryKey.of(catalogTable.getTableSchema().getPrimaryKey().getPrimaryKey(), newPrimaryKeyColumnNames);
            newtableSchema =
                    TableSchema.builder()
                            .columns(newColumns)
                            .primaryKey(newPrimaryKey)
                            .constraintKey(catalogTable.getTableSchema().getConstraintKeys())
                            .build();
        }
        else {
            newtableSchema =
                    TableSchema.builder()
                            .columns(newColumns)
                            .constraintKey(catalogTable.getTableSchema().getConstraintKeys())
                            .build();
        }

        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        finalDatabaseName,
                        finalSchemaName,
                        finalTableName);
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        newtableSchema,
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getComment(),
                        catalogTable.getCatalogName());
        Map<String, String> map = config.toMap();
        if (catalogTable.getTableId().getSchemaName() != null) {
            map.put(
                    TABLE.key(),
                    catalogTable.getTableId().getSchemaName()
                    + "."
                    + catalogTable.getTableId().getTableName());
        }
        else {
            map.put(TABLE.key(), catalogTable.getTableId().getTableName());
        }
        map.put(DATABASE.key(), catalogTable.getTableId().getDatabaseName());
        if (!config.getOptional(PRIMARY_KEYS).isPresent()) {
            if (primaryKey != null && !CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
                map.put(PRIMARY_KEYS.key(), String.join(",", primaryKey.getColumnNames()));
            }
            else {
                Optional<ConstraintKey> keyOptional =
                        catalogTable.getTableSchema().getConstraintKeys().stream()
                                .filter(
                                        key ->
                                                ConstraintKey.ConstraintType.UNIQUE_KEY.equals(
                                                        key.getConstraintType()))
                                .findFirst();
                if (keyOptional.isPresent()) {
                    map.put(
                            PRIMARY_KEYS.key(),
                            keyOptional.get().getColumnNames().stream()
                                    .map(key -> key.getColumnName())
                                    .collect(Collectors.joining(",")));
                }
            }
        }
        config = ReadonlyConfig.fromMap(new HashMap<>(map));
        // always execute
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcOptions.FIELD_IDE);
        catalogTable
                .getOptions()
                .put("fieldIde", fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        sinkConfig.getJdbcConnectionConfig().getUrl(),
                        sinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        dialect.connectionUrlParse(
                sinkConfig.getJdbcConnectionConfig().getUrl(),
                sinkConfig.getJdbcConnectionConfig().getProperties(),
                dialect.defaultParameter());
        CatalogTable finalCatalogTable = catalogTable;
        // get saveMode
        DataSaveMode dataSaveMode = config.get(DATA_SAVE_MODE);
        SchemaSaveMode schemaSaveMode = config.get(SCHEMA_SAVE_MODE);
        return () ->
                new JdbcSink(
                        options,
                        sinkConfig,
                        dialect,
                        schemaSaveMode,
                        dataSaveMode,
                        finalCatalogTable);
    }

    /**
     * recordOperation=true 时追加操作标识、操作时间两列，列名以目标库实际存在的列为准
     * （忽略大小写匹配），缺任一列直接报错。
     */
    private void appendOperationColumns(
            List<Column> newColumns,
            JdbcConnectionConfig connectionConfig,
            String databaseName,
            String schemaName,
            String tableName) {
        Map<String, String> realColumns =
                queryRealColumns(connectionConfig, databaseName, schemaName, tableName);
        String flagColumnName = realColumns.get(OPERATE_FLAG_COLUMN);
        String timeColumnName = realColumns.get(OPERATE_TIME_COLUMN);
        List<String> missingColumns = new ArrayList<>();
        if (flagColumnName == null) {
            missingColumns.add(OPERATE_FLAG_COLUMN);
        }
        if (timeColumnName == null) {
            missingColumns.add(OPERATE_TIME_COLUMN);
        }
        if (!missingColumns.isEmpty()) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED,
                    String.format(
                            "目标表 [%s] 缺少字段 %s，recordOperation=true 时这些字段必须存在",
                            buildTableLabel(databaseName, schemaName, tableName), missingColumns));
        }
        newColumns.add(
                PhysicalColumn.of(flagColumnName, BasicType.STRING_TYPE, 30, false, "I", ""));
        newColumns.add(
                PhysicalColumn.of(
                        timeColumnName,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        30,
                        false,
                        new Date(),
                        ""));
    }

    /** 读取目标表真实列名，返回 key 为列名小写、value 为数据库原始列名的映射。 */
    private Map<String, String> queryRealColumns(
            JdbcConnectionConfig connectionConfig,
            String databaseName,
            String schemaName,
            String tableName) {
        SimpleJdbcConnectionProvider connectionProvider =
                new SimpleJdbcConnectionProvider(connectionConfig);
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            Map<String, String> columns =
                    readColumns(connection, databaseName, schemaName, tableName);
            if (columns.isEmpty()) {
                // 部分驱动对 catalog/schema 匹配严格，放宽后仅按表名再查一次兜底
                columns = readColumns(connection, null, null, tableName);
            }
            if (columns.isEmpty()) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.NO_SUPPORT_OPERATION_FAILED,
                        String.format(
                                "无法读取目标表 [%s] 的列信息，请确认表已存在",
                                buildTableLabel(databaseName, schemaName, tableName)));
            }
            return columns;
        } catch (SQLException | ClassNotFoundException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                    String.format(
                            "读取目标表 [%s] 列信息失败",
                            buildTableLabel(databaseName, schemaName, tableName)),
                    e);
        } finally {
            connectionProvider.closeConnection();
        }
    }

    private Map<String, String> readColumns(
            Connection connection, String catalog, String schema, String table)
            throws SQLException {
        Map<String, String> columns = new HashMap<>();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet resultSet = metaData.getColumns(catalog, schema, table, null)) {
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                if (StringUtils.isNotBlank(columnName)) {
                    columns.put(columnName.toLowerCase(), columnName);
                }
            }
        }
        return columns;
    }

    private String buildTableLabel(String databaseName, String schemaName, String tableName) {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(databaseName)) {
            sb.append(databaseName).append(".");
        }
        if (StringUtils.isNotBlank(schemaName)) {
            sb.append(schemaName).append(".");
        }
        sb.append(tableName);
        return sb.toString();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, DRIVER, SCHEMA_SAVE_MODE, DATA_SAVE_MODE)
                .optional(
                        USER,
                        PASSWORD,
                        CONNECTION_CHECK_TIMEOUT_SEC,
                        BATCH_SIZE,
                        IS_EXACTLY_ONCE,
                        GENERATE_SINK_SQL,
                        AUTO_COMMIT,
                        SUPPORT_UPSERT_BY_QUERY_PRIMARY_KEY_EXIST,
                        PRIMARY_KEYS,
                        COMPATIBLE_MODE,
                        MULTI_TABLE_SINK_REPLICA)
                .conditional(
                        IS_EXACTLY_ONCE,
                        true,
                        XA_DATA_SOURCE_CLASS_NAME,
                        MAX_COMMIT_ATTEMPTS,
                        TRANSACTION_TIMEOUT_SEC)
                .conditional(IS_EXACTLY_ONCE, false, MAX_RETRIES)
                .conditional(GENERATE_SINK_SQL, true, DATABASE)
                .conditional(GENERATE_SINK_SQL, false, QUERY)
                .conditional(DATA_SAVE_MODE, DataSaveMode.CUSTOM_PROCESSING, CUSTOM_SQL)
                .build();
    }
}
