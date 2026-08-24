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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * ClickHouse SSL：仅当 use_ssl=true 时写入 PEM 临时文件并注入 ssl/sslmode/sslrootcert。
 * 老作业没有这两个参数时原样返回 URL，不改连接属性。
 */
public final class ClickHouseSslHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSslHelper.class);
    private static final String DEFAULT_SSL_MODE = "strict";
    private static final String JDBC_V1_FLAG = "clickhouse.jdbc.v1=true";
    private static final String TLS_SESSION_TICKET_PROPERTY =
            "jdk.tls.client.enableSessionTicketExtension";

    private ClickHouseSslHelper() {}

    public static String prepare(
            String dbType, String url, Properties props, Boolean useSsl, String caCert) {
        return prepare(dbType, url, props, useSsl, caCert, null);
    }

    public static String prepare(
            String dbType,
            String url,
            Properties props,
            Boolean useSsl,
            String caCert,
            String sslMode) {
        if (!Boolean.TRUE.equals(useSsl) || !isClickHouse(dbType, url)) {
            return url;
        }
        if (StringUtils.isBlank(caCert)) {
            throw new IllegalArgumentException("ClickHouse SSL 已开启，但未配置 ca_cert（PEM 证书文本）");
        }
        applySslProperties(props, caCert, sslMode);
        return forceJdbcV1(url);
    }

    private static String forceJdbcV1(String url) {
        if (StringUtils.isBlank(url) || StringUtils.containsIgnoreCase(url, JDBC_V1_FLAG)) {
            return url;
        }
        return url + (url.contains("?") ? "&" : "?") + JDBC_V1_FLAG;
    }

    private static void applySslProperties(Properties props, String caCertPem, String sslMode) {
        if (props == null) {
            throw new IllegalArgumentException("连接属性 Properties 不能为空");
        }
        Path certPath = writePemToTempFile(caCertPem);
        String certLocation = certPath.toAbsolutePath().toString();
        props.setProperty("ssl", "true");
        props.setProperty("sslmode", StringUtils.defaultIfBlank(sslMode, DEFAULT_SSL_MODE));
        props.setProperty("sslrootcert", certLocation);
        if (isNoneSslMode(sslMode)) {
            // 0.9.8 + HttpClient5 5.3：session resume 会 getPeerCertificates 失败
            props.setProperty("http_connection_provider", "HTTP_URL_CONNECTION");
            disableTlsSessionTicketIfUnset();
        }
        LOG.info("ClickHouse SSL sslrootcert 已生成: {}", certLocation);
    }

    private static void disableTlsSessionTicketIfUnset() {
        if (System.getProperty(TLS_SESSION_TICKET_PROPERTY) != null) {
            return;
        }
        System.setProperty(TLS_SESSION_TICKET_PROPERTY, "false");
        LOG.info("已关闭 TLS session ticket，避免 HttpClient5 二次握手 peer not authenticated");
    }

    private static boolean isNoneSslMode(String sslMode) {
        return StringUtils.isNotBlank(sslMode) && "none".equalsIgnoreCase(sslMode.trim());
    }

    private static Path writePemToTempFile(String caCertPem) {
        try {
            Path path = Files.createTempFile("clickhouse-ssl-ca-", ".pem");
            Files.write(path, caCertPem.trim().getBytes(StandardCharsets.UTF_8));
            path.toFile().deleteOnExit();
            return path;
        } catch (Exception e) {
            throw new IllegalArgumentException("写入 ClickHouse CA 证书临时文件失败: " + e.getMessage(), e);
        }
    }

    private static boolean isClickHouse(String dbType, String url) {
        if (StringUtils.isNotBlank(dbType)) {
            return "clickhouse".equalsIgnoreCase(dbType.trim());
        }
        return StringUtils.containsIgnoreCase(url, "jdbc:clickhouse");
    }
}
