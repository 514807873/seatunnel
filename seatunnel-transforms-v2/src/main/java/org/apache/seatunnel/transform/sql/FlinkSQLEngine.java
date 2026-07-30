package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Custom SQL engine used when config engine=FLINK. Aligned to jsqlparser 4.9 API used by 2.3.13.
 */
public class FlinkSQLEngine implements SQLEngine {
    private String inputTableName;
    @Nullable private String catalogTableName;
    private SeaTunnelRowType inputRowType;

    private String sql;
    private PlainSelect selectBody;
    private FlinkSQLType flinkSQLType;

    private Integer allColumnsCount = null;

    @Override
    public void init(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            String sql) {
        this.inputTableName = inputTableName;
        this.catalogTableName = catalogTableName;
        this.inputRowType = inputRowType;
        this.sql = sql;

        List<ZetaUDF> udfList = new ArrayList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(ZetaUDF.class, classLoader).forEach(udfList::add);

        this.flinkSQLType = new FlinkSQLType(inputRowType);
        parseSQL();
    }

    private void parseSQL() {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            this.selectBody = (PlainSelect) ((Select) statement).getSelectBody();
        } catch (JSQLParserException e) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("SQL parse failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    private void validateSQL(Statement statement) {
        try {
            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("Only supported DQL(select) SQL");
            }
            Select select = (Select) statement;
            if (!(select.getSelectBody() instanceof PlainSelect)) {
                throw new IllegalArgumentException("Unsupported SQL syntax");
            }
            PlainSelect body = (PlainSelect) select.getSelectBody();

            FromItem fromItem = body.getFromItem();
            if (fromItem instanceof Table) {
                Table table = (Table) fromItem;
                if (table.getSchemaName() != null) {
                    throw new IllegalArgumentException("Unsupported schema syntax");
                }
                if (table.getAlias() != null) {
                    throw new IllegalArgumentException("Unsupported table alias name syntax");
                }
                String tableName = table.getName();
                if (!inputTableName.equalsIgnoreCase(tableName)
                        && !tableName.equalsIgnoreCase(catalogTableName)) {
                    throw new IllegalArgumentException(
                            String.format("Table name: %s not found", tableName));
                }
            } else {
                throw new IllegalArgumentException("Unsupported sub table syntax");
            }

            if (body.getJoins() != null) {
                throw new IllegalArgumentException("Unsupported table join syntax");
            }
            if (body.getOrderByElements() != null) {
                throw new IllegalArgumentException("Unsupported ORDER BY syntax");
            }
            if (body.getGroupBy() != null) {
                throw new IllegalArgumentException("Unsupported GROUP BY syntax");
            }
            if (body.getLimit() != null || body.getOffset() != null) {
                throw new IllegalArgumentException("Unsupported LIMIT,OFFSET syntax");
            }
            for (SelectItem<?> selectItem : body.getSelectItems()) {
                if (selectItem.getExpression() instanceof AllColumns) {
                    throw new IllegalArgumentException("Unsupported all columns select syntax");
                }
            }
        } catch (Exception e) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("SQL validate failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    @Override
    public SeaTunnelRowType typeMapping(List<String> inputColumnsMapping) {
        List<SelectItem<?>> selectItems = selectBody.getSelectItems();
        int columnsSize = countColumnsSize(selectItems);

        String[] fieldNames = new String[columnsSize];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[columnsSize];
        if (inputColumnsMapping != null) {
            for (int i = 0; i < columnsSize; i++) {
                inputColumnsMapping.add(null);
            }
        }

        List<String> inputColumnNames =
                Arrays.stream(inputRowType.getFieldNames()).collect(Collectors.toList());

        int idx = 0;
        for (SelectItem<?> selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                for (int i = 0; i < inputRowType.getFieldNames().length; i++) {
                    fieldNames[idx] = inputRowType.getFieldName(i);
                    seaTunnelDataTypes[idx] = inputRowType.getFieldType(i);
                    if (inputColumnsMapping != null) {
                        inputColumnsMapping.set(idx, inputRowType.getFieldName(i));
                    }
                    idx++;
                }
            } else {
                Expression expression = selectItem.getExpression();
                if (selectItem.getAlias() != null) {
                    fieldNames[idx] = selectItem.getAlias().getName();
                } else if (expression instanceof Column) {
                    fieldNames[idx] = ((Column) expression).getColumnName();
                } else {
                    fieldNames[idx] = expression.toString();
                }

                if (inputColumnsMapping != null
                        && expression instanceof Column
                        && inputColumnNames.contains(((Column) expression).getColumnName())) {
                    inputColumnsMapping.set(idx, ((Column) expression).getColumnName());
                }

                seaTunnelDataTypes[idx] = flinkSQLType.getExpressionType(expression);
                idx++;
            }
        }
        return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
    }

    @Override
    public List<SeaTunnelRow> transformBySQL(
            SeaTunnelRow inputRow, SeaTunnelRowType outputRowType) {
        Object[] inputFields = scanTable(inputRow);
        Object[] outputFields = project(inputFields);

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(outputFields);
        seaTunnelRow.setRowKind(inputRow.getRowKind());
        seaTunnelRow.setTableId(inputRow.getTableId());
        return Collections.singletonList(seaTunnelRow);
    }

    private Object[] scanTable(SeaTunnelRow inputRow) {
        return inputRow.getFields();
    }

    private Object[] project(Object[] inputFields) {
        List<SelectItem<?>> selectItems = selectBody.getSelectItems();
        int columnsSize = countColumnsSize(selectItems);
        Object[] fields = new Object[columnsSize];

        int idx = 0;
        for (SelectItem<?> selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                for (Object inputField : inputFields) {
                    fields[idx] = inputField;
                    idx++;
                }
            } else {
                // Projection expression evaluation is not enabled in this custom engine path yet.
                idx++;
            }
        }
        return fields;
    }

    private int countColumnsSize(List<SelectItem<?>> selectItems) {
        if (allColumnsCount != null) {
            return allColumnsCount;
        }
        int allColumnsCnt = 0;
        for (SelectItem<?> selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                allColumnsCnt++;
            }
        }
        allColumnsCount =
                selectItems.size()
                        + inputRowType.getFieldNames().length * allColumnsCnt
                        - allColumnsCnt;
        return allColumnsCount;
    }
}
