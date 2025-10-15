package com.qh.myconnect.sink;

import com.qh.myconnect.config.MidCount;
import com.qh.myconnect.config.QualityFieldRule;
import com.qh.myconnect.config.SubTaskStatus;
import com.qh.myconnect.converter.CodeConverter;
import com.qh.myconnect.dialect.ClickHouse.ClickHouseDialect;
import com.qh.myconnect.dialect.trino.TrinoDialect;
import com.xjgreat.quality.checker.common.check.RuleChecker;
import com.xjgreat.quality.checker.common.check.SimpleRuleChecker;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.config.PreConfig;
import com.qh.myconnect.config.StatisticalLog;
import com.qh.myconnect.config.Util;
import com.qh.myconnect.converter.ColumnMapper;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MySinkWriterZipper extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private final Context context;
    private final List<SeaTunnelRow> cld = new ArrayList<>();
    private final MidCount midCount = new MidCount();
    private final JdbcSinkConfig jdbcSinkConfig;
    private final JobContext jobContext;

    private final JdbcDialect jdbcDialect;

    private final LocalDateTime startTime;
    private final Map<String, String> metaDataHash;

    private final Connection conn;
    private final String originTable;
    private final String zipperTable;
    private final List<ColumnMapper> columnMappers = new ArrayList<>();

    private SeaTunnelRowType sinkTableRowType;

    private final Util util = new Util();
    private PreConfig preConfig;

    private final Integer currentTaskId;
    private final RuleChecker ruleChecker = SimpleRuleChecker.newInstance();
    private final Set sqlErrorType = new HashSet();
    private final Set<String> ignoreColumns = new HashSet<>();
    private boolean isTrino = false;

    public MySinkWriterZipper(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, LocalDateTime startTime) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.context = context;
        this.currentTaskId = context.getIndexOfSubtask();
        log.info("currentTaskId:" + this.currentTaskId);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.originTable = this.jdbcSinkConfig.getTable();
        this.zipperTable = this.jdbcSinkConfig.getPreConfig().getZipperTableName();
        this.preConfig = jdbcSinkConfig.getPreConfig();
        if (preConfig != null && preConfig.getIgnoreTstamp()) {
            ignoreColumns.add("TSTAMP");
        }
        if (preConfig != null && preConfig.getIgnoreColumns() != null) {
            ignoreColumns.addAll(preConfig.getIgnoreColumns());
        }
        this.startTime = startTime;
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
        if (jdbcDialect instanceof TrinoDialect) {
            try (Statement statement = conn.createStatement()) {
                String setSessionQuery = "SET SESSION hive.insert_existing_partitions_behavior = 'APPEND'";
                statement.execute(setSessionQuery);
                this.conn.setAutoCommit(true);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set session", e);
            }
        }
        else {
            this.conn.setAutoCommit(false);
        }
        if (jdbcDialect instanceof TrinoDialect) {
            isTrino = true;
            try (Statement statement = conn.createStatement()) {
                String setSessionQuery = "SET SESSION hive.insert_existing_partitions_behavior = 'APPEND'";
                statement.execute(setSessionQuery);
                this.conn.setAutoCommit(true);
            } catch (Exception e) {
                throw new RuntimeException("Failed to set session", e);
            }
        }
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);
        String sqlQuery = jdbcDialect.getSinkQueryUpdate(this.columnMappers, 0, jdbcSinkConfig);
        PreparedStatement preparedStatementQuery = conn.prepareStatement(sqlQuery);
        ResultSet resultSet = preparedStatementQuery.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, String> metaDataHash = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            metaDataHash.put(metaData.getColumnName(i + 1), metaData.getColumnTypeName(i + 1));
        }
        this.metaDataHash = metaDataHash;
        if (!isTrino) {
            conn.commit();
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        List<ColumnMapper> needDecodeColumnMappers =
                this.columnMappers.stream().filter(x -> x.getDecodeConverter() != null).collect(Collectors.toList());
        for (ColumnMapper needDecodeColumnMapper : needDecodeColumnMappers) {
            element.setField(needDecodeColumnMapper.getSourceRowPosition(),
                    needDecodeColumnMapper.getDecodeConverter().apply(element.getField(needDecodeColumnMapper.getSourceRowPosition())));
        }
        midCount.setWriteCount(midCount.getWriteCount() + 1);
        if (this.jdbcSinkConfig.isOpenQuality()) {
            List<QualityFieldRule> rules = this.jdbcSinkConfig.getQualityFieldRule();
            boolean bo = false;
            for (QualityFieldRule rule : rules) {
                Integer sourceRowPosition = columnMappers.stream()
                        .filter(x -> x.getSinkColumnName().equalsIgnoreCase(rule.getColumnName()))
                        .findAny()
                        .get()
                        .getSourceRowPosition();
                Object field = element.getField(sourceRowPosition);
                try {
                    bo = SimpleRuleChecker.checkAssert(ruleChecker, rule.getTableinfoId(), rule.getFieldinfoId(), field);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (bo) {
                    midCount.plusQualityCount();
                    return;
                }
            }
        }
        this.cld.add(element);
        if (midCount.getWriteCount() % this.jdbcSinkConfig.getBatchSize() == 0 || this.jobContext.getJobMode().equals(JobMode.STREAMING)) {
            this.jdbcDialect.insertToDb(this.columnMappers,
                    this.jdbcSinkConfig,
                    this.conn,
                    this.metaDataHash,
                    this.cld,
                    this.util,
                    this.jobContext,
                    this.sqlErrorType,
                    midCount
            );
            cld.clear();
        }
    }

    @Override
    public void close() {
        try {
            this.jdbcDialect.insertToDb(this.columnMappers,
                    this.jdbcSinkConfig,
                    this.conn,
                    this.metaDataHash,
                    this.cld,
                    this.util,
                    this.jobContext,
                    this.sqlErrorType,
                    midCount
            );
            statisticalResults();
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void statisticalResults() throws Exception {
        try {
            SubTaskStatus subTaskStatus = new SubTaskStatus();
            subTaskStatus.setFlinkJobId(this.jobContext.getJobId());
            subTaskStatus.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
            subTaskStatus.setDbSchema(this.jdbcSinkConfig.getDbSchema());
            subTaskStatus.setTableName(this.jdbcSinkConfig.getTable());
            subTaskStatus.setSubtaskIndexId(this.currentTaskId + "");
            subTaskStatus.setStatus("done");
            util.setSubTaskStatus(subTaskStatus);
            List<SubTaskStatus> subTaskStatus1 = util.getSubTaskStatus(subTaskStatus);
            int done = (int) subTaskStatus1.stream().filter(x -> !x.getStatus().equalsIgnoreCase("done")).count();
            log.info("subTaskStatus:" + done);
            if (done == 0) {
                List<ColumnMapper> ucColumns =
                        this.columnMappers.stream()
                                .filter(ColumnMapper::isUc)
                                .collect(Collectors.toList());
                //处理删除数据
                {
                    long del = 0;
                    if (StringUtils.isBlank(this.jdbcSinkConfig.getPreConfig().getClusterName())) {
                        Optional<String> dbSchema = Optional.ofNullable(this.jdbcSinkConfig.getDbSchema());
                        String zipperTableName = zipperTable;
                        String originTableName = originTable;
                        if (dbSchema.isPresent() && StringUtils.isNoneBlank(dbSchema.get())) {
                            zipperTableName = this.jdbcSinkConfig.getDbSchema() + "." + zipperTable;
                            originTableName = this.jdbcSinkConfig.getDbSchema() + "." + originTable;
                        }
                        del =
                                this.jdbcDialect.deleteDataZipper(
                                        jdbcSinkConfig,
                                        conn,
                                        zipperTableName,
                                        originTableName,
                                        this.columnMappers,
                                        ucColumns);
                    }
                    else {
                        del =
                                this.jdbcDialect.deleteDataZipperCluster(
                                        jdbcSinkConfig,
                                        conn,
                                        zipperTable,
                                        originTable,
                                        this.columnMappers,
                                        ucColumns,
                                        this.jdbcSinkConfig.getPreConfig().getClusterName());
                    }
                    conn.commit();
                    midCount.setDeleteCount(del);
                }
                compareTables(conn, this.zipperTable);
                //处理新增数据
                {
                    List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
                    List<String> ucs =
                            columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
                    String insertSqlCount =
                            this.jdbcDialect.insertDataCountZipper(jdbcSinkConfig, originTable, ucs);
                    ResultSet resultSet = conn.createStatement().executeQuery(insertSqlCount);
                    resultSet.next();
                    midCount.setInsertCount(resultSet.getLong(1));
                    String insertSql = this.jdbcDialect.insertDataZipper(jdbcSinkConfig, originTable, columns, ucs,
                            conn);
                    conn.createStatement().execute(insertSql);
                    conn.commit();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        StatisticalLog statisticalLog = new StatisticalLog();
        LocalDateTime endTime = LocalDateTime.now();
        statisticalLog.setFlinkJobId(this.jobContext.getJobId());
        statisticalLog.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
        if (this.jdbcSinkConfig.getDbSchema() != null) {
            statisticalLog.setDbSchema(this.jdbcSinkConfig.getDbSchema());
        }
        statisticalLog.setTableName(this.jdbcSinkConfig.getTable());
        statisticalLog.setWriteCount(midCount.getWriteCount());
        statisticalLog.setQualityCount(midCount.getQualityCount());
        statisticalLog.setModifyCount(midCount.getUpdateCount());
        statisticalLog.setDeleteCount(midCount.getDeleteCount());
        statisticalLog.setInsertCount(midCount.getInsertCount());
        statisticalLog.setKeepCount(midCount.getKeepCount());
        statisticalLog.setErrorCount(midCount.getErrorCount());
        statisticalLog.setStartTime(startTime);
        statisticalLog.setEndTime(endTime);
        util.insertLog(statisticalLog);
    }

    private boolean containsAtLeastTwoDotsRegex(String str) {
        Pattern pattern = Pattern.compile("\\..*\\.");
        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }

    private void initColumnMappers(JdbcSinkConfig jdbcSinkConfig, SeaTunnelRowType sourceRowType, SeaTunnelRowType sinkTableRowType, Connection conn) throws SQLException {
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        Map<String, String> codeMapper = jdbcSinkConfig.getCodeMapper();
        Map<String, String> decodeMapper = jdbcSinkConfig.getDecodeMapper();
        Map<String, String> dmMap = new HashMap<>();
        List<String> allDms = new ArrayList<>();
        if (codeMapper != null) {
            allDms = codeMapper.values().stream().filter(x -> x.startsWith("DM")).distinct().collect(Collectors.toList());
        }
        for (String allDm : allDms) {
            String[] split = allDm.split("\\.");
            String sql = String.format("select %s,%s from %s", split[2], split[3], split[1]);
            try (Connection con = util.getPanguConnection(); Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    dmMap.put(allDm + "." + rs.getString(split[2]), rs.getString(split[3]));
                }
            }
        }
        fieldMapper.forEach((sourceColumnName, targetColumnName) -> {
            CodeConverter converter = new CodeConverter();
            if (codeMapper != null) {
                String any = codeMapper.get(targetColumnName) == null ? codeMapper.get(sourceColumnName) : codeMapper.get(targetColumnName);
                if (any != null) {
                    if (any.startsWith("ENCRYPT.") && containsAtLeastTwoDotsRegex(any)) {
                        converter = new CodeConverter(targetColumnName, any.split("\\.")[2]);
                    }
                }
            }
            converter.setDmMap(dmMap);
            ColumnMapper columnMapper = new ColumnMapper();
            columnMapper.setSourceColumnName(sourceColumnName);
            columnMapper.setSourceRowPosition(sourceRowType.indexOf(sourceColumnName));
            String typeName = sourceRowType.getFieldType(sourceRowType.indexOf(sourceColumnName)).getTypeClass().getName();
            columnMapper.setSourceColumnTypeName(typeName);
            columnMapper.setSinkColumnName(targetColumnName);
            columnMapper.setSinkRowPosition(sinkTableRowType.indexOf(targetColumnName));
            String typeNameSK = sinkTableRowType.getFieldType(sinkTableRowType.indexOf(targetColumnName)).getTypeClass().getName();
            columnMapper.setSinkColumnTypeName(typeNameSK);

            for (String primaryKey : jdbcSinkConfig.getPrimaryKeys()) {
                if (primaryKey.equalsIgnoreCase(targetColumnName)) {
                    columnMapper.setUc(true);
                }
            }
            try {
                ResultSetMetaData metaData = this.jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    if (targetColumnName.equalsIgnoreCase(columnName)) {
                        String columnTypeName = metaData.getColumnTypeName(i + 1);
                        columnMapper.setSinkColumnDbType(columnTypeName);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (codeMapper != null) {
                String safeCode = codeMapper.get(targetColumnName) == null ? codeMapper.get(sourceColumnName) : codeMapper.get(targetColumnName);
                if (safeCode != null && StringUtils.isNoneBlank(safeCode)) {
                    if (safeCode.startsWith("DM")) {
                        columnMapper.setConverter(converter.dmConverter(safeCode));
                    }
                    else if (safeCode.startsWith("ENCRYPT")) {
                        columnMapper.setConverter(converter.encryptConverter(safeCode));
                    }
                    else if (safeCode.startsWith("FUNCTION.")) {
                        int dotIndex = safeCode.indexOf('.');
                        String function = safeCode.substring(dotIndex + 1);
                        columnMapper.setValueSupplier(() -> function.replace(":value", "?"));
                    }
                }
            }
            CodeConverter decodeConverter = new CodeConverter();
            if (decodeMapper != null) {
                String decode = decodeMapper.get(targetColumnName) == null ? decodeMapper.get(sourceColumnName) : decodeMapper.get(targetColumnName);
                if (decode != null) {
                    if (decode.startsWith("DECRYPT.") && containsAtLeastTwoDotsRegex(decode)) {
                        decodeConverter = new CodeConverter(targetColumnName, decode.split("\\.")[2]);
                    }
                    columnMapper.setDecodeConverter(decodeConverter.decryptConverter(decode));
                }
            }
            columnMappers.add(columnMapper);
        });
    }

    private void compareTables(Connection conn, String tableName) throws SQLException {
        int indexOfSubtask = context.getIndexOfSubtask();
        log.info("subtask:{} compare table:{}", indexOfSubtask, tableName);
        List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        List<String> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        String querySource = this.jdbcDialect.getDataSql(this.jdbcSinkConfig, this.columnMappers, this.originTable);
        String queryTarget = this.jdbcDialect.getDataSqlZipper(this.jdbcSinkConfig, this.columnMappers, tableName);
        try (Connection conn1 = util.getConnection(jdbcSinkConfig); Connection conn2 =
                util.getConnection(jdbcSinkConfig)) {
            try (Statement stmt1 = conn1.createStatement(); Statement stmt2 = conn2.createStatement(); ResultSet rs1 = stmt1.executeQuery(querySource); ResultSet rs2 = stmt2.executeQuery(queryTarget)) {
                ResultSetMetaData md1 = rs1.getMetaData();
                int columnCount = md1.getColumnCount();
                StringBuilder sourceKey = new StringBuilder();
                StringBuilder targetKey = new StringBuilder();
                boolean rs1Done = false;
                boolean rs2Done = false;
                if (rs1.next()) {
                    for (String ucColumn : ucColumns) {
                        String object = util.Object2String(rs1.getObject(ucColumn));
                        sourceKey.append(object);
                    }
                }
                if (rs2.next()) {
                    for (String ucColumn : ucColumns) {
                        String object = util.Object2String(rs2.getObject(ucColumn));
                        targetKey.append(object);
                    }
                }

//            if (sourceKey.length() == 0) {
//                log.info("全部是删除");
//            }
//            if (targetKey.length() == 0) {
//                log.info("全收是新增");
//            }
                if (!sourceKey.toString().isEmpty() && !targetKey.toString().isEmpty()) {
                    while (true) {
                        int result = sourceKey.toString().compareTo(targetKey.toString());
                        if (result == 0) {
//                        log.info("主键相等");
                            //对比数据
                            boolean change = false;
                            for (int i = 1; i <= columnCount; i++) {
                                Object value1 = rs1.getObject(i);
                                Object value2 = rs2.getObject(i);
                                if (!Objects.equals(value1, value2)) {
//                                    log.info("发现字段区别:" + md1.getColumnName(i));
//                                    log.info("新值: " + value1);
//                                    log.info("老值: " + value2);
//                                    if (ignoreColumns.contains(md1.getColumnName(i))) {
//                                        log.info("该字段已加入忽略对比:" + md1.getColumnName(i));
//                                    }
                                    if (!ignoreColumns.contains(md1.getColumnName(i))) {
                                        String updateSql = jdbcDialect.updateTableSqlZipper(jdbcSinkConfig, ucColumns
                                                , conn);
                                        String modifyTableSql = jdbcDialect.insertModifyTableSql(jdbcSinkConfig, originTable,
                                                columns,
                                                ucColumns,
                                                conn);
                                        PreparedStatement preparedStatement = conn.prepareStatement(updateSql);
                                        PreparedStatement preparedStatement1 = conn.prepareStatement(modifyTableSql);
                                        for (int j = 0; j < ucColumns.size(); j++) {
                                            preparedStatement.setObject(j + 1, rs2.getObject(ucColumns.get(j)));
                                            preparedStatement1.setObject(j + 1, rs2.getObject(ucColumns.get(j)));
                                        }
                                        preparedStatement.executeUpdate();
                                        preparedStatement1.executeUpdate();
                                        preparedStatement.close();
                                        conn.commit();
                                        change = true;
                                    }
                                }
                                if (change) break;
                            }
                            if (change) {
                                midCount.setUpdateCount(midCount.getUpdateCount() + 1);
                            }
                            else {
                                midCount.setKeepCount(midCount.getKeepCount() + 1);
                            }
//                        log.info("同时往下移动");
                            if (rs1.next()) {
                                sourceKey.setLength(0);
                                for (String ucColumn : ucColumns) {
                                    String object = util.Object2String(rs1.getObject(ucColumn));
                                    sourceKey.append(object);
                                }
                            }
                            else {
                                rs1Done = true;
//                            while (rs2.next()) {
//                                log.info("剩下的rs2全部要删除");
//                            }
                                rs2Done = true;
                            }
                            if (rs2.next()) {
                                targetKey.setLength(0);
                                for (String ucColumn : ucColumns) {
                                    String object = util.Object2String(rs2.getObject(ucColumn));
                                    targetKey.append(object);
                                }
                            }
                            else {
                                rs2Done = true;
//                            while (rs1.next()) {
//                                log.info("剩下的rs1全部要插入");
//                            }
                                rs1Done = true;
                            }
                        }
                        if (result > 0) {
//                        log.info("rs2需要往下移,删除rs2");
                            if (rs2.next()) {
                                targetKey.setLength(0);
                                for (String ucColumn : ucColumns) {
                                    String object = util.Object2String(rs2.getObject(ucColumn));
                                    targetKey.append(object);
                                }
                            }
                            else {
                                rs2Done = true;
//                            while (rs1.next()) {
//                                log.info("剩下的rs1全部要插入");
//                            }
                                rs1Done = true;
                            }
                        }
                        if (result < 0) {
//                        log.info("rs1需要往下移,插入rs1");
//                        log.info("sourceKey " + sourceKey);
                            if (rs1.next()) {
                                sourceKey.setLength(0);
                                for (String ucColumn : ucColumns) {
                                    String object = util.Object2String(rs1.getObject(ucColumn));
                                    sourceKey.append(object);
                                }
                            }
                            else {
                                rs1Done = true;
//                            while (rs2.next()) {
//                                log.info("剩下的rs2全部要删除");
//                            }
                                rs2Done = true;
                            }
                        }
                        if (rs1Done && rs2Done) {
                            break;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
