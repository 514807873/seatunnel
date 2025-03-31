package com.qh.myconnect.config;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class QualityFieldRule implements Serializable {


    private String id;

    private String fieldinfoId;


    private String checkruleId;


    private String tableinfoId;

    private Date createTime;

    private String columnName;

}
