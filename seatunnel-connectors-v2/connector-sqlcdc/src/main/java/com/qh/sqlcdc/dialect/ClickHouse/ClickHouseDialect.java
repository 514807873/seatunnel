package com.qh.sqlcdc.dialect.ClickHouse;

import com.qh.sqlcdc.converter.JdbcRowConverter;
import com.qh.sqlcdc.dialect.JdbcDialect;
import com.qh.sqlcdc.dialect.JdbcDialectTypeMapper;

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
        return new ClickHouseMapper();
    }
}
