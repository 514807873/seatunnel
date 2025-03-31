package com.qh.myconnect.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.util.OptionMark;

//import com.clickhouse.jdbc.internal.ClickHouseConnectionImpl;
import com.qh.myconnect.dialect.JdbcDialectFactory;
import lombok.Data;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

@Data
public class PreConfig implements Serializable {
    private static final long serialVersionUID = -1L;

    @OptionMark(description = "插入模式")
    private String insertMode; // 插入模式 全量 complete  增量 increment

    @OptionMark(description = "全量模式是否清空表 true清 false不清")
    private boolean cleanTableWhenComplete;

    @OptionMark(description = "无数据输入继续清空表 true清 false不清")
    private boolean cleanTableWhenCompleteNoDataIn = false;

    @OptionMark(description = "增量模式 update或者zipper模式 ")
    private String incrementMode;

    @OptionMark(description = "增量模式 忽略时间戳比对 ")
    private Boolean ignoreTstamp = true;

    @OptionMark(description = "增量模式 忽略对比的字段 ")
    private List<String> ignoreColumns;

    @OptionMark(description = "ck 集群模式下 集群的名字")
    private String clusterName;

    @OptionMark(description = "增量更新模式下 是否开启数据删除操作")
    private boolean openDelete = true;

    @OptionMark(description = "增量拉链模式下 拉链表的表名称")
    private String zipperTableName;

    @OptionMark(description = "增量拉链模式下 拉链表三个字段名")
    private List<String> zipperColumns;
    @OptionMark(description = "增量拉链模式下 插入修改删除3个字段的值(默认I,U,D)")
    private List<String> zipperFlagValue;

    @OptionMark(description = "自动时间戳")
    private boolean autoTimestamp = false;

    @OptionMark(description = "自动时间戳字段名")
    private String autoTimestampColumnName;


    @OptionMark(description = "记录操作类型")
    private boolean recordOperate = false;

    @OptionMark(description = "操作类型字段名")
    private String recordOperateColumnName;

    @OptionMark(description = "预执行sql")
    private String preSql;

    public PreConfig() {
    }

    public void doPreConfig(Connection connection, JdbcSinkConfig jdbcSinkConfig)
            throws SQLException {
        String tableName = jdbcSinkConfig.getTable();
        String schemaPattern = jdbcSinkConfig.getDbSchema();
        if (Objects.equals(schemaPattern, "")) {
            schemaPattern = null;
        }

        if (this.insertMode.equalsIgnoreCase("complete")
            && this.cleanTableWhenComplete
            && this.cleanTableWhenCompleteNoDataIn) {
            try (Statement st = connection.createStatement()) {
                st.execute(JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType()).truncateTable(jdbcSinkConfig));
            }
        }
        if(this.insertMode.equalsIgnoreCase("complete") && !this.cleanTableWhenComplete ){
            if(StringUtils.isNotBlank(this.preSql)){
                PreparedStatement preparedStatement = connection.prepareStatement(this.preSql);
                preparedStatement.execute();
            }
        }

        if (this.insertMode.equalsIgnoreCase("increment") && null != this.incrementMode && this.incrementMode.equalsIgnoreCase("update")) {
            if (null == jdbcSinkConfig.getPrimaryKeys()
                || jdbcSinkConfig.getPrimaryKeys().isEmpty()) {
                throw new RuntimeException(String.format("增量更新模式下,未标示逻辑主键", tableName));
            }
            String tmpTableName = "XJ$_" + tableName;
            String copyTableOnlyColumnSql =
                    JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                            .copyTableOnlyColumn(tableName, tmpTableName, jdbcSinkConfig);
            if (clusterName != null && !clusterName.equalsIgnoreCase("")) {
                String dropSqlCluster =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTableOnCluster(
                                        jdbcSinkConfig,
                                        jdbcSinkConfig.getDatabase(),
                                        tmpTableName,
                                        clusterName);
                copyTableOnlyColumnSql =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .copyTableOnlyColumnOnCluster(
                                        tableName,
                                        tmpTableName,
                                        jdbcSinkConfig,
                                        clusterName,
                                        jdbcSinkConfig.getDatabase()
                                );
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSqlCluster);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println("删除报错意味着没有表");
                }
            }
            else {
                String dropSql =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTable(jdbcSinkConfig, tmpTableName);
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSql);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println(dropSql + "删除报错意味着没有表" + e.getMessage());
                }
            }
            PreparedStatement preparedStatement1 =
                    connection.prepareStatement(copyTableOnlyColumnSql);
            preparedStatement1.execute();
            preparedStatement1.close();
            if (!jdbcSinkConfig.getDbType().equalsIgnoreCase("clickhouse")) {
                PreparedStatement preparedStatement2 =
                        connection.prepareStatement(
                                JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                        .createIndex(tmpTableName, jdbcSinkConfig));
                try {
                    preparedStatement2.execute();
                } catch (SQLException e) {
                    System.out.println("无法创建索引,不影响作业运行,运行效率会变慢");
                }finally {
                    preparedStatement2.close();
                }
            }
            if (jdbcSinkConfig.getDbType().equalsIgnoreCase("pgsql")) {
                PreparedStatement preparedStatement = connection.prepareStatement(
                        String.format("ALTER TABLE  \"%s\".\"%s\" REPLICA IDENTITY FULL", jdbcSinkConfig.getDbSchema(),
                                tmpTableName));
                preparedStatement.execute();
                preparedStatement.close();
            }
        }

        if (this.insertMode.equalsIgnoreCase("increment") && null != this.incrementMode && this.incrementMode.equalsIgnoreCase("zipper")) {
            try (Statement st = connection.createStatement()) {
                st.execute(JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType()).truncateTable(jdbcSinkConfig));
            }
        }
    }

    public void dropUcTable(Connection connection, JdbcSinkConfig jdbcSinkConfig) throws SQLException {
        String tableName = jdbcSinkConfig.getTable();
        if (this.insertMode.equalsIgnoreCase("increment")) {
            String tmpTableName = "XJ$_" + tableName;
            String copyTableOnlyColumnSql =
                    JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                            .copyTableOnlyColumn(tableName, tmpTableName, jdbcSinkConfig);
            if (clusterName != null && !clusterName.equalsIgnoreCase("")) {
                String dropSqlCluster =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTableOnCluster(
                                        jdbcSinkConfig,
                                        jdbcSinkConfig.getDatabase(),
                                        tmpTableName,
                                        clusterName);
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSqlCluster);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println("删除报错意味着没有表");
                }
            }
            else {
                String dropSql =
                        JdbcDialectFactory.getJdbcDialect(jdbcSinkConfig.getDbType())
                                .dropTable(jdbcSinkConfig, tmpTableName);
                try {
                    PreparedStatement drop = connection.prepareStatement(dropSql);
                    drop.execute();
                    drop.close();
                } catch (SQLException e) {
                    System.out.println(dropSql + "删除报错意味着没有表" + e.getMessage());
                }
            }
        }
    }
}
