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

package com.qh.sqlcdc.dialect;

import org.stringtemplate.v4.ST;

import com.qh.sqlcdc.converter.JdbcRowConverter;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */
public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    JdbcRowConverter getRowConverter();

    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

    default String checkRealDelete(List<String> keys, String nativeSql) {
        String template =
                "select count(1) sl from (<query>) a where <keys:{key | <key> = ? }; separator=\" and \"> ";
        ST st = new ST(template);
        st.add("keys", keys);
        st.add("query", nativeSql);
        return st.render();
    }
}
