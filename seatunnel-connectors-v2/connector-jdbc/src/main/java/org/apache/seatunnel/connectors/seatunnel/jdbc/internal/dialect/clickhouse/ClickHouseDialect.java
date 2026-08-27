package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class ClickHouseDialect implements JdbcDialect {
    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new ClickHouseJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new ClickHouseTypeMapper();
    }

    /**
     * Native ClickHouse JDBC (housepower) only exposes column metadata after execute. Keep the
     * 2.3.5-qinhuan wrapper so getMetaData() is not called on a null ResultSet.
     */
    @Override
    public ResultSetMetaData getResultSetMetaData(Connection conn, String query)
            throws SQLException {
        PreparedStatement ps =
                conn.prepareStatement(String.format("select * from  (%s) a where 1=2 ", query));
        ps.executeQuery();
        return ps.getMetaData();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public String getUpdateStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] conditionFields,
            boolean isPrimaryKeyUpdated) {
        fieldNames =
                Arrays.stream(fieldNames)
                        .filter(
                                fieldName ->
                                        isPrimaryKeyUpdated
                                                || !Arrays.asList(conditionFields)
                                                        .contains(fieldName))
                        .toArray(String[]::new);

        Set<String> difference = new HashSet<>(Arrays.asList(fieldNames));
        difference.removeAll(new HashSet<>(Arrays.asList(conditionFields)));
        String[] newFieldNames = difference.toArray(new String[0]);

        String setClause =
                Arrays.stream(newFieldNames)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "ALTER TABLE %s UPDATE %s WHERE %s settings mutations_sync = 1",
                tableIdentifier(database, tableName), setClause, conditionClause);
    }

    @Override
    public String getDeleteStatement(String database, String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "ALTER TABLE %s DELETE WHERE %s settings mutations_sync = 1",
                tableIdentifier(database, tableName), conditionClause);
    }
}
