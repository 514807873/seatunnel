/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qh.sqlcdc.config;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 由平台注入的 JDBC 目标配置，用于 SqlCdc 与目标表对账。
 */
@Data
public class DirectSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String url;
    private String user;
    private String password;
    private String driver;
    private String dbType;
    private String query;
    /** sourceColumn -> targetColumn */
    private Map<String, String> fieldMapper = new LinkedHashMap<>();
    /** 目标表主键（目标字段名） */
    private List<String> primaryKeys = new ArrayList<>();
}
