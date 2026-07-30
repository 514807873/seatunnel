package com.qh.sqlcdc.converter;

import lombok.Data;

@Data
public class ColumnMapper {
    private String sourceColumnName;
    private String sourceColumnTypeName;
    private String sinkColumnName;
    private String sinkColumnTypeName;
    private Integer sourceRowPosition;
    private Integer sinkRowPosition;
    private boolean uc = false;
    private String sinkColumnDbType;
}
