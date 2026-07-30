package com.qh.sqlcdc.dialect;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public interface JdbcDialectTypeMapper {
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException;
}
