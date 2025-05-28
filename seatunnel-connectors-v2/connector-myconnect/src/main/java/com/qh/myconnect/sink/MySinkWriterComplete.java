package com.qh.myconnect.sink;

import com.qh.myconnect.config.MidCount;
import com.qh.myconnect.config.QualityFieldRule;
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
import com.qh.myconnect.config.TruncateTable;
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
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class MySinkWriterComplete extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType sourceRowType;
    private final List<SeaTunnelRow> cld = new ArrayList<>();
    private final MidCount midCount = new MidCount();
    private final JdbcSinkConfig jdbcSinkConfig;
    private final JobContext jobContext;

    private final JdbcDialect jdbcDialect;

    private final LocalDateTime startTime;
    private final Map<String, String> metaDataHash;

    private final Connection conn;

    private final List<ColumnMapper> columnMappers = new ArrayList<>();

    private SeaTunnelRowType sinkTableRowType;

    private final Util util = new Util();

    private final Long tableCount;

    private final PreConfig preConfig;

    private final Integer currentTaskId;

    private final Set<String> sqlErrorType = new HashSet();

    private boolean isTrino = false;
    private final RuleChecker ruleChecker = SimpleRuleChecker.newInstance();


    public MySinkWriterComplete(SeaTunnelRowType seaTunnelRowType, Context context, ReadonlyConfig config, JobContext jobContext, Long tableCount) throws SQLException {

        this.jobContext = jobContext;
        this.sourceRowType = seaTunnelRowType;
        this.currentTaskId = context.getIndexOfSubtask();
        log.info("currentTaskId:" + this.currentTaskId);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.preConfig = jdbcSinkConfig.getPreConfig();
        this.startTime = LocalDateTime.now();
        this.jdbcDialect = JdbcDialectFactory.getJdbcDialect(this.jdbcSinkConfig.getDbType());
        this.conn = util.getConnection(this.jdbcSinkConfig);
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
        else {
            this.conn.setAutoCommit(false);
        }
        this.sinkTableRowType = util.initTableField(conn, this.jdbcDialect, this.jdbcSinkConfig);
        this.initColumnMappers(this.jdbcSinkConfig, this.sourceRowType, this.sinkTableRowType, conn);
        this.tableCount = tableCount;
        if (this.preConfig.isCleanTableWhenComplete() && this.preConfig.isCleanTableWhenCompleteNoDataIn()) {
            midCount.setDeleteCount(this.tableCount);
        }
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
        assert ruleChecker != null;
        midCount.setWriteCount(midCount.getWriteCount() + 1);
        if (midCount.getWriteCount() == 1 && this.preConfig.isCleanTableWhenComplete()) {
            TruncateTable truncateTable = new TruncateTable();
            truncateTable.setFlinkJobId(this.jobContext.getJobId());
            truncateTable.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
            if (this.jdbcSinkConfig.getDbSchema() != null && !this.jdbcSinkConfig.getDbSchema().equalsIgnoreCase("")) {
                if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("oracle") || this.jdbcSinkConfig.getDbType().equalsIgnoreCase("pgsql")) {
                    truncateTable.setDbSchema(this.jdbcSinkConfig.getDbSchema());
                    truncateTable.setTableName("\"" + this.jdbcSinkConfig.getDbSchema() + "\"" + "." + "\"" + this.jdbcSinkConfig.getTable() + "\"");
                }
                else {
                    truncateTable.setDbSchema(this.jdbcSinkConfig.getDbSchema());
                    truncateTable.setTableName(this.jdbcSinkConfig.getDbSchema() + "." + this.jdbcSinkConfig.getTable());
                }
            }
            else {
                if (this.jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                    truncateTable.setTableName(String.format("`%s`", this.jdbcSinkConfig.getTable()));
                }
                else {
                    truncateTable.setTableName(this.jdbcSinkConfig.getTable());
                }
            }
            util.truncateTable(truncateTable);
            midCount.setDeleteCount(this.tableCount);
        }
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
            // 自动更新时间戳
            if (jdbcSinkConfig.getPreConfig().isAutoTimestamp()) {
                if (StringUtils.isNoneBlank(jdbcSinkConfig.getPreConfig().getAutoTimestampColumnName())) {
                    String sql = jdbcDialect.modifyTimestamp(jdbcSinkConfig, conn);
                    conn.prepareStatement(sql).execute();
                    conn.commit();
                }
            }
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void statisticalResults() throws Exception {
        LocalDateTime endTime = LocalDateTime.now();
        StatisticalLog statisticalLog = new StatisticalLog();
        statisticalLog.setFlinkJobId(this.jobContext.getJobId());
        statisticalLog.setDataSourceId(this.jdbcSinkConfig.getDbDatasourceId());
        if (this.jdbcSinkConfig.getDbSchema() != null) {
            statisticalLog.setDbSchema(this.jdbcSinkConfig.getDbSchema());
        }
        statisticalLog.setTableName(this.jdbcSinkConfig.getTable());
        statisticalLog.setWriteCount(midCount.getWriteCount());
        statisticalLog.setQualityCount(midCount.getQualityCount());
        statisticalLog.setModifyCount(0L);
        statisticalLog.setDeleteCount(midCount.getDeleteCount());
        statisticalLog.setInsertCount(midCount.getInsertCount());
        statisticalLog.setKeepCount(0L);
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
                String any = codeMapper.get(targetColumnName);
                if (any != null) {
                    if (any.startsWith("ENCRYPT.") && containsAtLeastTwoDotsRegex(any)) {
                        converter = new CodeConverter(targetColumnName,any.split("\\.")[2]);
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
                String safeCode = codeMapper.get(targetColumnName);
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
                String decode = decodeMapper.get(targetColumnName);
                if (decode != null) {
                    if (decode.startsWith("DECRYPT.") && containsAtLeastTwoDotsRegex(decode)) {
                        decodeConverter = new CodeConverter(targetColumnName,decode.split("\\.")[2]);
                    }
                    columnMapper.setDecodeConverter(decodeConverter.decryptConverter(decode));
                }
            }
            columnMappers.add(columnMapper);
        });
    }
}
