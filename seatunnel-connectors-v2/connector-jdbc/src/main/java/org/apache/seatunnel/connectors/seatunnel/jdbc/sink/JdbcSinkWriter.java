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

import com.alibaba.fastjson.JSONObject;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.StreamRecord;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Slf4j
public class JdbcSinkWriter implements SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState>, SupportMultiTableSinkWriter<ConnectionPoolManager> {
    private JdbcOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> outputFormat;
    private final JdbcDialect dialect;
    private final TableSchema tableSchema;
    private JdbcConnectionProvider connectionProvider;
    private transient boolean isOpen;
    private final Integer primaryKeyIndex;
    private final JdbcSinkConfig jdbcSinkConfig;

    private Long insertCount = 0L;

    private Long deleteCount = 0l;

    private Long updateCount = 0L;

    private final String flinkJobId;

    private CodeConverter converter = new CodeConverter();

    private final Connection panguConnection = this.getPanguConnection();

    private final DSLContext dslContext = DSL.using(panguConnection, SQLDialect.MYSQL);

    private StreamRecord streamRecord = new StreamRecord(System.getenv("seaTunnelJobId"));

    public JdbcSinkWriter(JdbcDialect dialect, JdbcSinkConfig jdbcSinkConfig, TableSchema tableSchema, Integer primaryKeyIndex, String flinkJobId) {
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.primaryKeyIndex = primaryKeyIndex;
        this.connectionProvider = dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat = new JdbcOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, tableSchema).build();
        this.flinkJobId = flinkJobId;
        List<String> allDms = new ArrayList<>();
        Map<String, String> dmMap = new HashMap<>();
        Map<String, String> codeMapper = jdbcSinkConfig.getCodeMapper();
        if (codeMapper != null) {
            allDms = codeMapper.values().stream().filter(x -> x.startsWith("DM")).distinct().collect(Collectors.toList());
            for (String allDm : allDms) {
                String[] split = allDm.split("\\.");
                String sql = String.format("select %s,%s from %s", split[2], split[3], split[1]);
                try (Statement stmt = panguConnection.createStatement()) {
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        dmMap.put(allDm + "." + rs.getString(split[2]), rs.getString(split[3]));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String rq = now.format(formatter);
        dslContext.insertInto(table("pangu.seatunnel_stream_record"))
                .columns(
                        field("jobId"),
                        field("rq"),
                        field("writeCount"),
                        field("insertCount"),
                        field("updateCount"),
                        field("deleteCount")
                )
                .values(
                        streamRecord.getSeatunnelId(),
                        rq,
                        streamRecord.getWriteCount(),
                        streamRecord.getInsertCount(),
                        streamRecord.getUpdateCount(),
                        streamRecord.getDeleteCount()
                ).onDuplicateKeyUpdate()
                .set(field("writeCount"), streamRecord.getWriteCount())
                .set(field("insertCount"), streamRecord.getInsertCount())
                .set(field("updateCount"), streamRecord.getUpdateCount())
                .set(field("deleteCount"), streamRecord.getDeleteCount())
                .execute();
        converter.setDmMap(dmMap);
        startLog();
    }

    @Override
    public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(int tableSize, int queueSize) {
        HikariDataSource ds = new HikariDataSource();
        ds.setIdleTimeout(30 * 1000);
        ds.setMaximumPoolSize(queueSize);
        ds.setJdbcUrl(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUsername().isPresent()) {
            ds.setUsername(jdbcSinkConfig.getJdbcConnectionConfig().getUsername().get());
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getPassword().isPresent()) {
            ds.setPassword(jdbcSinkConfig.getJdbcConnectionConfig().getPassword().get());
        }
        ds.setAutoCommit(jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit());
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    @Override
    public void setMultiTableResourceManager(MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager, int queueIndex) {
        connectionProvider.closeConnection();
        this.connectionProvider = new SimpleJdbcConnectionPoolProviderProxy(multiTableResourceManager.getSharedResource().get(), jdbcSinkConfig.getJdbcConnectionConfig(), queueIndex);
        this.outputFormat = new JdbcOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, tableSchema).build();
    }

    @Override
    public Optional<Integer> primaryKey() {
        return primaryKeyIndex != null ? Optional.of(primaryKeyIndex) : Optional.empty();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String rq = now.format(formatter);
        if (element.getRowKind().equals(RowKind.INSERT)) {
            this.insertCount++;
            dslContext.update(table("pangu.seatunnel_stream_record"))
                    .set(field("writeCount", Integer.class), field("writeCount", Integer.class).add(1))
                    .set(field("insertCount", Integer.class), field("insertCount", Integer.class).add(1))
                    .where(field("jobId").eq(streamRecord.getSeatunnelId()))
                    .and(field("rq").eq(rq))
                    .execute();
        }
        else if (element.getRowKind().equals(RowKind.DELETE)) {
            this.deleteCount++;
            dslContext.update(table("pangu.seatunnel_stream_record"))
                    .set(field("writeCount", Integer.class), field("writeCount", Integer.class).add(1))
                    .set(field("deleteCount", Integer.class), field("deleteCount", Integer.class).add(1))
                    .where(field("jobId").eq(streamRecord.getSeatunnelId()))
                    .and(field("rq").eq(rq))
                    .execute();
        }
        else {
            this.updateCount++;
            dslContext.update(table("pangu.seatunnel_stream_record"))
                    .set(field("writeCount", Integer.class), field("writeCount", Integer.class).add(1))
                    .set(field("updateCount", Integer.class), field("updateCount", Integer.class).add(1))
                    .where(field("jobId").eq(streamRecord.getSeatunnelId()))
                    .and(field("rq").eq(rq))
                    .execute();
        }
        List<String> columns = tableSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        Object[] newFields = new Object[columns.size()];
        Map<String, String> valueMapper = jdbcSinkConfig.getValueMapper();
        Map<String, String> codeMapper = jdbcSinkConfig.getCodeMapper();
        if (valueMapper != null && !valueMapper.isEmpty()) {
            newFields = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                String valueIndex = getKeyByValue(valueMapper, columns.get(i));
                String code = codeMapper != null ? codeMapper.get(columns.get(i)) : null;
                if (valueIndex != null) {
                    if (code == null) {
                        newFields[i] = element.getField(Integer.parseInt(valueIndex));
                    }
                    else {
                        newFields[i] = converter.convert(code, String.valueOf(element.getField(Integer.parseInt(valueIndex))));
                    }
                }
            }
        }
//        System.arraycopy(element.getFields(), 0, newFields, 0, element.getArity());
        SeaTunnelRow newRow = new SeaTunnelRow(newFields);
        newRow.setRowKind(element.getRowKind());
        newRow.setTableId(element.getTableId());

        if (jdbcSinkConfig.getRecordOperation() != null && jdbcSinkConfig.getRecordOperation()) {
            if (element.getRowKind().equals(RowKind.INSERT)) {
                newRow.setField(newRow.getArity() - 2, "U");
            }
            else if (element.getRowKind().equals(RowKind.DELETE)) {
                newRow.setField(newRow.getArity() - 2, "D");
            }
            else if (element.getRowKind().equals(RowKind.UPDATE_AFTER)) {
                newRow.setField(newRow.getArity() - 2, "U");
            }
            else {
                newRow.setField(newRow.getArity() - 2, "U");
            }

            newRow.setField(newRow.getArity() - 1, LocalDateTime.now());
        }
        tryOpen();
        outputFormat.writeRecord(newRow);

    }

    private void startLog() {
        ScheduledExecutorService service = new ScheduledThreadPoolExecutor(1);
        service.scheduleAtFixedRate(() -> {
            JSONObject param = new JSONObject();
            param.put("flinkJobId", this.flinkJobId);
            param.put("dataSourceId", jdbcSinkConfig.getDbDatasourceId());
            if (this.jdbcSinkConfig.getDbSchema() != null) {
                param.put("dbSchema", this.jdbcSinkConfig.getDbSchema());
            }
            param.put("tableName", this.jdbcSinkConfig.getTable());
            param.put("insertCount", insertCount);
            param.put("modifyCount", updateCount);
            param.put("deleteCount", deleteCount);
            String st_log_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/gatherJobLog";
            try {
                this.sendPostRequest(st_log_url, param.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 1, 2, TimeUnit.SECONDS);
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED, "commit failed," + e.getMessage(), e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() throws IOException {
        tryOpen();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, "unable to close JDBC sink write", e);
        }
        outputFormat.close();
        try {
            panguConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <K, V> K getKeyByValue(Map<K, V> map, V value) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (entry.getValue().equals(value)) {
                return entry.getKey();
            }
        }
        return null; // 如果找不到，返回null
    }

    public String sendPostRequest(String url, String data) throws Exception {
        URL apiUrl = new URL(url);
        HttpURLConnection con = (HttpURLConnection) apiUrl.openConnection();
        String result = null;
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);
        OutputStream outputStream = con.getOutputStream();
        outputStream.write(data.getBytes());
        outputStream.flush();
        outputStream.close();

        // 处理响应
        int responseCode = con.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {

            InputStream is = con.getInputStream();
            // 缓冲流包装字符输入流,放入内存中,读取效率更快
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            StringBuffer stringBuffer1 = new StringBuffer();
            String line = null;
            while ((line = br.readLine()) != null) {
                // 将每次读取的行进行保存
                stringBuffer1.append(line);
            }
            result = stringBuffer1.toString();
        }
        else {
            // 处理失败响应
        }
        con.disconnect();
        return result;
    }

    public Connection getPanguConnection() {
        try {
            Driver driver = this.loadDriver("com.mysql.cj.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("user", System.getenv("PANGU_MYSQL_ROOT_USER"));
            info.setProperty("password", System.getenv("PANGU_MYSQL_ROOT_PASSWORD"));
            Connection conn = driver.connect(System.getenv("PANGU_MYSQL_URL"), info);
            if (conn == null) {
                throw new JdbcConnectorException(JdbcConnectorErrorCode.NO_SUITABLE_DRIVER, "业务mysql无法连接，请检查");
            }
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public java.sql.Driver loadDriver(String driverName) throws ClassNotFoundException {
        checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            java.sql.Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        Class<?> clazz = Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (java.sql.Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new JdbcConnectorException(JdbcConnectorErrorCode.CREATE_DRIVER_FAILED, "Fail to create driver of class " + driverName, ex);
        }
    }
}
