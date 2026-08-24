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

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Oracle TCPS/SSL：仅当 use_ssl=true 时改写 URL 并注入 trustStore。
 * 老作业没有 use_ssl / ca_cert 时原样返回，不改连接属性。
 */
public final class OracleSslHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSslHelper.class);
    private static final String DEFAULT_TRUST_STORE_PASSWORD = "changeit";
    private static final Pattern SERVICE_URL =
            Pattern.compile("^jdbc:oracle:thin:@([^:/?]+):(\\d+)/([^?;]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SID_URL =
            Pattern.compile("^jdbc:oracle:thin:@([^:/?]+):(\\d+):([^?;]+)", Pattern.CASE_INSENSITIVE);

    private OracleSslHelper() {}

    public static String prepare(
            String dbType, String url, Properties props, Boolean useSsl, String caCert) {
        if (!Boolean.TRUE.equals(useSsl) || !isOracle(dbType, url)) {
            return url;
        }
        if (StringUtils.isBlank(caCert)) {
            throw new IllegalArgumentException("Oracle SSL 已开启，但未配置 ca_cert（PEM 证书文本）");
        }
        String tcpsUrl = toTcpsUrl(url);
        applyTrustStore(props, caCert, DEFAULT_TRUST_STORE_PASSWORD, false);
        return tcpsUrl;
    }

    static String toTcpsUrl(String url) {
        if (StringUtils.isBlank(url)) {
            return url;
        }
        if (StringUtils.containsIgnoreCase(url, "PROTOCOL=tcps")) {
            return url;
        }
        String base = url;
        int q = url.indexOf('?');
        if (q >= 0) {
            base = url.substring(0, q);
        }
        Matcher serviceMatcher = SERVICE_URL.matcher(base);
        if (serviceMatcher.find()) {
            return buildTcpsUrl(
                    serviceMatcher.group(1),
                    Integer.parseInt(serviceMatcher.group(2)),
                    serviceMatcher.group(3),
                    true);
        }
        Matcher sidMatcher = SID_URL.matcher(base);
        if (sidMatcher.find()) {
            return buildTcpsUrl(
                    sidMatcher.group(1),
                    Integer.parseInt(sidMatcher.group(2)),
                    sidMatcher.group(3),
                    false);
        }
        throw new IllegalArgumentException("无法将 Oracle JDBC URL 转换为 TCPS 格式: " + url);
    }

    private static String buildTcpsUrl(String host, Integer port, String database, boolean serviceName) {
        String connectData =
                serviceName ? "(SERVICE_NAME=" + database + ")" : "(SID=" + database + ")";
        return String.format(
                "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=%s)(PORT=%s))(CONNECT_DATA=%s))",
                host, port, connectData);
    }

    private static void applyTrustStore(
            Properties props, String caCertPem, String trustStorePassword, boolean sslServerDnMatch) {
        if (props == null) {
            throw new IllegalArgumentException("连接属性 Properties 不能为空");
        }
        Path trustStorePath = buildTrustStoreFromPem(caCertPem, trustStorePassword);
        String trustStoreLocation = trustStorePath.toAbsolutePath().toString();
        props.setProperty("javax.net.ssl.trustStore", trustStoreLocation);
        props.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
        props.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());
        props.setProperty("oracle.net.ssl_server_dn_match", String.valueOf(sslServerDnMatch));
        LOG.info("Oracle TCPS trustStore 已生成: {}", trustStoreLocation);
    }

    private static Path buildTrustStoreFromPem(String caCertPem, String trustStorePassword) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate cert =
                    cf.generateCertificate(
                            new ByteArrayInputStream(caCertPem.getBytes(StandardCharsets.UTF_8)));

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            trustStore.setCertificateEntry("oracle_ca", cert);

            char[] password =
                    (trustStorePassword == null ? DEFAULT_TRUST_STORE_PASSWORD : trustStorePassword)
                            .toCharArray();
            Path path = Files.createTempFile("oracle-tcps-truststore-", ".jks");
            try (OutputStream out = Files.newOutputStream(path)) {
                trustStore.store(out, password);
            }
            path.toFile().deleteOnExit();
            return path;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("解析 Oracle CA 证书并生成 trustStore 失败: " + e.getMessage(), e);
        }
    }

    private static boolean isOracle(String dbType, String url) {
        if (StringUtils.isNotBlank(dbType)) {
            return "oracle".equalsIgnoreCase(dbType.trim());
        }
        return StringUtils.containsIgnoreCase(url, "jdbc:oracle");
    }
}
