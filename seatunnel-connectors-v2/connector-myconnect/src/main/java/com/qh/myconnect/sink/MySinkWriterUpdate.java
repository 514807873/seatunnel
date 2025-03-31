package com.qh.myconnect.sink;

import com.alibaba.fastjson2.JSONWriter;
import com.qh.myconnect.config.MidCount;
import com.qh.myconnect.config.QualityFieldRule;
import com.qh.myconnect.config.SubTaskStatus;
import com.qh.myconnect.converter.CodeConverter;
import com.xjgreat.quality.checker.common.check.RuleChecker;
import com.xjgreat.quality.checker.common.check.SimpleRuleChecker;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import com.alibaba.fastjson2.JSON;
import com.qh.myconnect.config.JdbcSinkConfig;
import com.qh.myconnect.config.PreConfig;
import com.qh.myconnect.config.SeaTunnelJobsHistoryErrorRecord;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MySinkWriterUpdate extends AbstractSinkWriter<SeaTunnelRow, Void> {
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

    private final String table;
    private final List<ColumnMapper> columnMappers = new ArrayList<>();

    private SeaTunnelRowType sinkTableRowType;

    private final Util util = new Util();
    private final PreConfig preConfig;
    private final RuleChecker ruleChecker = SimpleRuleChecker.newInstance();
    private final Integer currentTaskId;

    private final Set sqlErrorType = new HashSet();
    private final String tmpTable;
    private CodeConverter converter = new CodeConverter();
    private Set<String> ignoreColumns = new HashSet<>();

    public MySinkWriterUpdate(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, LocalDateTime startTime) throws SQLException {
        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.context = context;
        this.currentTaskId = context.getIndexOfSubtask();
        log.info("currentTaskId:" + this.currentTaskId);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        if (jdbcSinkConfig.getDriver().equalsIgnoreCase("com.github.housepower.jdbc.ClickHouseDriver")) {
            this.jdbcSinkConfig.setBatchSize(50000);
        }
        if (jdbcSinkConfig.getBatchSize() == 0) {
            this.jdbcSinkConfig.setBatchSize(2000);
        }
        this.tmpTable = "XJ$_" + this.jdbcSinkConfig.getTable();
        this.table = this.jdbcSinkConfig.getTable();
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
        this.conn.setAutoCommit(false);
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
        conn.commit();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
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
            this.insertToDb();
            cld.clear();
        }
    }

    @Override
    public void close() {
        try {
            this.insertToDb();
            statisticalResults(conn);
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void insertToDb() {
        String sql = null;
        try {
            List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            sql = jdbcDialect.insertTmpTableSql(this.jdbcSinkConfig, columns, values);
            PreparedStatement psUpsert = conn.prepareStatement(sql);
            boolean hasError = false;
            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = this.columnMappers.get(i).getConverter().apply(seaTunnelRow.getField(valueIndex));
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    try {
                        psUpsert.addBatch();
                    } catch (SQLException e) {
                        hasError = true;
                        break;
                    }
                }
            }
            if (hasError) {
                throw new RuntimeException();
            }
            psUpsert.executeBatch();
            conn.commit();
            psUpsert.clearBatch();
            psUpsert.close();

        } catch (Exception e) {
            log.error("错误sql:" + sql, ExceptionUtils.getStackTrace(e));
            try {
                conn.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            insertToDbOneByOne();
        }
    }

    private void statisticalResults(Connection conn) throws Exception {
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
                log.info("开始处理删除数据");
                {
                    if (preConfig.isOpenDelete()) {
                        long del = 0;
                        if (StringUtils.isNoneBlank(this.jdbcSinkConfig.getDbSchema())) {
                            del =
                                    this.jdbcDialect.deleteData(
                                            conn,
                                            this.jdbcSinkConfig.getDbSchema() + "." + table,
                                            this.jdbcSinkConfig.getDbSchema() + "." + tmpTable,
                                            ucColumns);
                        }
                        else if (StringUtils.isNoneBlank(this.jdbcSinkConfig.getPreConfig().getClusterName())) {
                            del =
                                    this.jdbcDialect.deleteDataOnCluster(
                                            conn,
                                            table,
                                            tmpTable,
                                            ucColumns,
                                            this.jdbcSinkConfig.getPreConfig().getClusterName());
                        }
                        else {
                            del = this.jdbcDialect.deleteData(conn, table, tmpTable, ucColumns);
                        }
                        conn.commit();
                        midCount.setDeleteCount(del + midCount.getDeleteCount());
                    }
                    else {
                        if (preConfig.isAutoTimestamp() && preConfig.isRecordOperate()) {

                            //处理原来删除又被插回来的数据
                            jdbcDialect.deleteReInsertData(conn, table, tmpTable, ucColumns, jdbcSinkConfig);
                            long del = 0;
                            if (StringUtils.isNoneBlank(this.jdbcSinkConfig.getDbSchema())) {
                                del =
                                        this.jdbcDialect.deleteDataLogic(
                                                conn,
                                                this.jdbcSinkConfig.getDbSchema() + "." + table,
                                                this.jdbcSinkConfig.getDbSchema() + "." + tmpTable,
                                                ucColumns,
                                                this.preConfig);
                            }
                            else if (StringUtils.isNoneBlank(this.jdbcSinkConfig.getPreConfig().getClusterName())) {
                                del =
                                        this.jdbcDialect.deleteDataOnClusterLogic(
                                                conn,
                                                table,
                                                tmpTable,
                                                ucColumns,
                                                this.jdbcSinkConfig.getPreConfig().getClusterName(),
                                                this.preConfig
                                        );
                            }
                            else {
                                del = this.jdbcDialect.deleteDataLogic(conn, table, tmpTable, ucColumns, this.preConfig);
                            }
                            conn.commit();
                            midCount.setDeleteCount(del + midCount.getDeleteCount());
                        }
                    }
                    conn.commit();
                }
                log.info("删除数据处理完毕");
                log.info("开始对比数据");
                compareTables(this.table);
                log.info("数据对比完毕");
                //处理新增数据
                log.info("处理新增数据");
                {
                    List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
                    List<String> ucs =
                            columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
                    String insertSqlCount =
                            this.jdbcDialect.insertDataCount(jdbcSinkConfig, tmpTable, ucs);
                    ResultSet resultSet = conn.createStatement().executeQuery(insertSqlCount);
                    resultSet.next();
                    midCount.setInsertCount(resultSet.getLong(1));
                    String insertSql = this.jdbcDialect.insertData(jdbcSinkConfig, tmpTable, columns, ucs);
                    conn.createStatement().execute(insertSql);
                    conn.commit();
                }
                log.info("新增数据处理完毕");
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

    private void insertToDbOneByOne() {
        try {
            List<String> columns = this.columnMappers.stream().map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
            List<String> values = this.columnMappers.stream().map(x -> "?").collect(Collectors.toList());
            String sql = jdbcDialect.insertTmpTableSql(this.jdbcSinkConfig, columns, values);
            for (SeaTunnelRow seaTunnelRow : this.cld) {
                if (seaTunnelRow != null) {
                    PreparedStatement psUpsert = conn.prepareStatement(sql);
                    for (int i = 0; i < this.columnMappers.size(); i++) {
                        Integer valueIndex = this.columnMappers.get(i).getSourceRowPosition();
                        Object field = this.columnMappers.get(i).getConverter().apply(seaTunnelRow.getField(valueIndex));
                        String column = columns.get(i);
                        String dbType = metaDataHash.get(column);
                        jdbcDialect.setPreparedStatementValueByDbType(i + 1, psUpsert, dbType, util.Object2String(field));
                    }
                    try {
                        psUpsert.addBatch();
                        psUpsert.executeBatch();
                        conn.commit();
                        psUpsert.clearBatch();
                        psUpsert.close();
                    } catch (SQLException ee) {
                        midCount.setErrorCount(midCount.getErrorCount() + 1);
                        if (this.jobContext.getIsRecordErrorData() == 1 && midCount.getErrorCount() <= this.jobContext.getMaxRecordNumber() && !sqlErrorType.contains(ee.getMessage())) {
                            LinkedHashMap<String, Object> jsonObject = new LinkedHashMap<>();
                            for (int i = 0; i < this.columnMappers.size(); i++) {
                                jsonObject.put(this.columnMappers.get(i).getSourceColumnName(), seaTunnelRow.getField(i));
                            }
                            log.info(JSON.toJSONString(jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
                            SeaTunnelJobsHistoryErrorRecord errorRecord = new SeaTunnelJobsHistoryErrorRecord();
                            errorRecord.setFlinkJobId(this.jobContext.getJobId());
                            errorRecord.setDataSourceId(jdbcSinkConfig.getDbDatasourceId());
                            errorRecord.setDbSchema(jdbcSinkConfig.getDbSchema());
                            errorRecord.setTableName(jdbcSinkConfig.getTable());
                            errorRecord.setErrorData(JSON.toJSONString(jsonObject, JSONWriter.Feature.WriteMapNullValue, JSONWriter.Feature.WriteNullListAsEmpty));
                            errorRecord.setErrorMessage(ExceptionUtils.getStackTrace(ee));
                            sqlErrorType.add(ee.getMessage());
                            try {
                                util.insertErrorData(errorRecord);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        }
                    } finally {
                        conn.commit();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean containsAtLeastTwoDotsRegex(String str) {
        Pattern pattern = Pattern.compile("\\..*\\.");
        Matcher matcher = pattern.matcher(str);
        return matcher.find();
    }

    private void initColumnMappers(JdbcSinkConfig jdbcSinkConfig, SeaTunnelRowType sourceRowType, SeaTunnelRowType sinkTableRowType, Connection conn) throws SQLException {
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        Map<String, String> codeMapper = jdbcSinkConfig.getCodeMapper();
        if (codeMapper != null) {
            Optional<String> any = codeMapper.values().stream().filter(x -> x.startsWith("ENCRYPT.") && containsAtLeastTwoDotsRegex(x)).findAny();
            any.ifPresent(x -> {
                converter = new CodeConverter(x.split("\\.")[2]);
            });
        }
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
        converter.setDmMap(dmMap);
        fieldMapper.forEach((k, v) -> {
            ColumnMapper columnMapper = new ColumnMapper();
            columnMapper.setSourceColumnName(k);
            columnMapper.setSourceRowPosition(sourceRowType.indexOf(k));
            String typeNameSS = sourceRowType.getFieldType(sourceRowType.indexOf(k)).getTypeClass().getName();
            columnMapper.setSourceColumnTypeName(typeNameSS);
            columnMapper.setSinkColumnName(v);
            columnMapper.setSinkRowPosition(sinkTableRowType.indexOf(v));
            String typeNameSK = sinkTableRowType.getFieldType(sinkTableRowType.indexOf(v)).getTypeClass().getName();
            columnMapper.setSinkColumnTypeName(typeNameSK);

            for (String primaryKey : jdbcSinkConfig.getPrimaryKeys()) {
                if (primaryKey.equalsIgnoreCase(v)) {
                    columnMapper.setUc(true);
                }
            }
            try {
                ResultSetMetaData metaData = this.jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    if (v.equalsIgnoreCase(columnName)) {
                        String columnTypeName = metaData.getColumnTypeName(i + 1);
                        columnMapper.setSinkColumnDbType(columnTypeName);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (codeMapper != null) {
                String safeCode = codeMapper.get(v);
                if (safeCode != null && StringUtils.isNoneBlank(safeCode)) {
                    if (safeCode.startsWith("DM")) {
                        columnMapper.setConverter(converter.dmConverter(safeCode));
                    }
                    else if (safeCode.startsWith("ENCRYPT")) {
                        columnMapper.setConverter(converter.encryptConverter(safeCode));
                    }
                }
            }
            columnMappers.add(columnMapper);
        });
    }

    private void compareTables(String tableName) throws SQLException {
        int indexOfSubtask = context.getIndexOfSubtask();
        log.info("subtask:{} compare table:{}", indexOfSubtask, tableName);
        List<String> ucColumns = columnMappers.stream().filter(ColumnMapper::isUc).map(ColumnMapper::getSinkColumnName).collect(Collectors.toList());
        String querySource = this.jdbcDialect.getDataSql(this.jdbcSinkConfig, this.columnMappers, this.tmpTable);
        String queryTarget = this.jdbcDialect.getDataSql(this.jdbcSinkConfig, this.columnMappers, tableName);
        try (Connection conn1 = util.getConnection(jdbcSinkConfig); Connection conn2 =
                util.getConnection(jdbcSinkConfig)) {
            try (Statement stmt1 = conn1.createStatement(); Statement stmt2 = conn2.createStatement(); ResultSet rs1 =
                    stmt1.executeQuery(querySource); ResultSet rs2 = stmt2.executeQuery(queryTarget)) {
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
                    int hang = 1;
                    while (true) {
                        int result = sourceKey.toString().compareTo(targetKey.toString());
                        if (result == 0) {
                            if (hang % 10000 == 0) {
                                log.info("已对比" + hang + "行");
                            }
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
                                    if (!ignoreColumns.contains(md1.getColumnName(i))) {
                                        String updateSql = jdbcDialect.updateTableSql(jdbcSinkConfig, md1.getColumnName(i), ucColumns);
                                        PreparedStatement preparedStatement = conn.prepareStatement(updateSql);
                                        preparedStatement.setObject(1, rs1.getObject(md1.getColumnName(i)));
                                        for (int j = 0; j < ucColumns.size(); j++) {
                                            preparedStatement.setObject(j + 2, rs2.getObject(ucColumns.get(j)));
                                        }
                                        preparedStatement.executeUpdate();
                                        preparedStatement.close();
                                        conn.commit();
                                        change = true;
                                    }
                                }
                            }
                            if (change) {
                                midCount.setUpdateCount(midCount.getUpdateCount() + 1);
                            }
                            else {
                                midCount.setKeepCount(midCount.getKeepCount() + 1);
                            }
//                        log.info("同时往下移动");
                            if (rs1.next()) {
                                hang += 1;
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
