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

import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class SqlCdcConfig implements Serializable {
    private String driver;
    private String query;
    private String url;
    private String user;
    private String passWord;
    private String dbType;
    private Boolean openDelete;
    private List<String> primaryKeys;
    public SqlCdcConfig(Config config) {
        this.driver = config.getString(SqlCdcConfigOptions.DRIVER.key());
        this.query = config.getString(SqlCdcConfigOptions.QUERY.key());
        this.url = config.getString(SqlCdcConfigOptions.URL.key());
        this.user = config.getString(SqlCdcConfigOptions.USER.key());
        this.passWord = config.getString(SqlCdcConfigOptions.PASSWORD.key());
        this.dbType = config.getString(SqlCdcConfigOptions.DBTYPE.key());
        this.openDelete  = config.getBoolean(SqlCdcConfigOptions.openDelete.key());
        this.primaryKeys = config.getStringList(SqlCdcConfigOptions.PRIMARY_KEYS.key());
    }
}
