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

package org.apache.seatunnel.connectors.seatunnel.xjjdbc.util;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.xjjdbc.config.XjJdbcSinkConfig;

import java.math.BigDecimal;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Properties;

/** Small helpers shared by the XjJdbc sink: connection creation and value-to-string fallback. */
public final class Util {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static final String SINK_STRING = "string";
    public static final String SINK_DATE = "date";
    public static final String SINK_TIMESTAMP = "timestamp";
    public static final String SINK_TIME = "time";
    public static final String SINK_NUMBER = "number";
    public static final String SINK_BOOLEAN = "boolean";
    public static final String SINK_BINARY = "binary";
    public static final String SINK_UNKNOWN = "unknown";

    private Util() {}

    public static Connection getConnection(XjJdbcSinkConfig config) {
        return getConnection(
                config.getUrl(),
                config.getDriver(),
                config.getUser(),
                config.getPassWord(),
                config.getDbType());
    }

    public static Connection getConnection(
            String url, String driverName, String user, String password) {
        return getConnection(url, driverName, user, password, null);
    }

    public static Connection getConnection(
            String url, String driverName, String user, String password, String dbType) {
        try {
            Driver driver = loadDriver(driverName, dbType);
            Properties info = new Properties();
            if (user != null) {
                info.setProperty("user", user);
            }
            if (password != null) {
                info.setProperty("password", password);
            }
            Connection conn = driver.connect(url, info);
            if (conn == null) {
                throw new RuntimeException("No suitable driver found for url: " + url);
            }
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("Failed to create connection for url: " + url, e);
        }
    }

    private static Driver loadDriver(String driverName, String dbType)
            throws ClassNotFoundException {
        if (StringUtils.isBlank(driverName)) {
            throw new IllegalArgumentException("driver must not be blank");
        }
        // Legacy targets need old JDBC drivers loaded from external jars under $OLD_DRIVER_HOME:
        // the modern bundled drivers cannot connect to SQL Server 2000 / MySQL 5 and would clash
        // with them on the classpath, so each is loaded through an isolated class loader.
        if ("sqlserver2000".equalsIgnoreCase(dbType)) {
            return loadDriverFromJar(oldDriverHome() + "/sqljdbc4-3.0.jar", driverName);
        }
        if ("com.mysql.jdbc.Driver".equalsIgnoreCase(driverName)) {
            return loadDriverFromJar(
                    oldDriverHome() + "/mysql-connector-java-5.1.43.jar", "com.mysql.jdbc.Driver");
        }
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create driver of class " + driverName, ex);
        }
    }

    private static String oldDriverHome() {
        String home = System.getenv("OLD_DRIVER_HOME");
        if (StringUtils.isBlank(home)) {
            throw new IllegalStateException(
                    "Environment variable OLD_DRIVER_HOME is required to load the legacy JDBC"
                            + " driver");
        }
        return home;
    }

    private static Driver loadDriverFromJar(String jarFile, String className) {
        try {
            String jarUrl = String.format("jar:file:%s!/", jarFile);
            ClassLoader parent = Util.class.getClassLoader().getParent();
            URLClassLoader loader = new URLClassLoader(new URL[] {new URL(jarUrl)}, parent);
            return (Driver)
                    Class.forName(className, true, loader).getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to load legacy driver " + className + " from " + jarFile, ex);
        }
    }

    /**
     * Classify a JDBC type name from sink metadata. Binary is checked before string so LONG RAW
     * does not fall into LONG.
     */
    public static String sinkKind(String sinkDbType) {
        if (StringUtils.isBlank(sinkDbType)) {
            return SINK_UNKNOWN;
        }
        String t = sinkDbType.toLowerCase();
        if (t.contains("blob")
                || t.contains("raw")
                || t.contains("binary")
                || t.contains("bytea")) {
            return SINK_BINARY;
        }
        if (t.contains("clob")
                || t.contains("text")
                || t.contains("varchar")
                || t.contains("char")
                || t.equals("long")
                || t.contains("string")) {
            return SINK_STRING;
        }
        if (t.contains("timestamp") || t.contains("datetime")) {
            return SINK_TIMESTAMP;
        }
        if (t.contains("date") && !t.contains("time")) {
            return SINK_DATE;
        }
        if (t.contains("time") && !t.contains("date") && !t.contains("stamp")) {
            return SINK_TIME;
        }
        if (t.contains("int")
                || t.contains("number")
                || t.contains("numeric")
                || t.contains("decimal")
                || t.contains("float")
                || t.contains("double")
                || t.contains("real")
                || t.contains("money")
                || t.contains("serial")) {
            return SINK_NUMBER;
        }
        if (t.contains("bool") || t.equals("bit")) {
            return SINK_BOOLEAN;
        }
        return SINK_UNKNOWN;
    }

    /** Same date/number rules as the fast job Object2String. */
    public static String object2String(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.sql.Time) {
            return TIME_FMT.format(((java.sql.Time) value).toLocalTime());
        }
        if (value instanceof java.sql.Date) {
            return DATE_FMT.format(((java.sql.Date) value).toLocalDate());
        }
        if (value instanceof java.sql.Timestamp) {
            return DATETIME_FMT.format(((java.sql.Timestamp) value).toLocalDateTime());
        }
        if (value.getClass().getName().toLowerCase().contains("timestamp")) {
            return formatOracleTimestamp(value);
        }
        if (value instanceof LocalTime) {
            return TIME_FMT.format((LocalTime) value);
        }
        if (value instanceof LocalDate) {
            return DATE_FMT.format((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return DATETIME_FMT.format((LocalDateTime) value);
        }
        if (value instanceof java.util.Date) {
            return DATETIME_FMT.format(
                    ((java.util.Date) value)
                            .toInstant()
                            .atZone(ZoneId.systemDefault())
                            .toLocalDateTime());
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).stripTrailingZeros().toPlainString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return String.valueOf(value);
    }

    public static java.sql.Timestamp toTimestamp(Object value) {
        if (value instanceof java.sql.Timestamp) {
            return (java.sql.Timestamp) value;
        }
        if (value instanceof LocalDateTime) {
            return java.sql.Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof LocalDate) {
            return java.sql.Timestamp.valueOf(((LocalDate) value).atStartOfDay());
        }
        if (value instanceof java.sql.Date) {
            return new java.sql.Timestamp(((java.sql.Date) value).getTime());
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime());
        }
        String text = object2String(value);
        if (text.length() <= 10) {
            return java.sql.Timestamp.valueOf(LocalDate.parse(text, DATE_FMT).atStartOfDay());
        }
        return java.sql.Timestamp.valueOf(LocalDateTime.parse(text, DATETIME_FMT));
    }

    public static java.sql.Date toSqlDate(Object value) {
        if (value instanceof java.sql.Date) {
            return (java.sql.Date) value;
        }
        if (value instanceof LocalDate) {
            return java.sql.Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalDateTime) {
            return java.sql.Date.valueOf(((LocalDateTime) value).toLocalDate());
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date) value).getTime());
        }
        return java.sql.Date.valueOf(LocalDate.parse(object2String(value).substring(0, 10), DATE_FMT));
    }

    public static java.sql.Time toSqlTime(Object value) {
        if (value instanceof java.sql.Time) {
            return (java.sql.Time) value;
        }
        if (value instanceof LocalTime) {
            return java.sql.Time.valueOf((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return java.sql.Time.valueOf(((LocalDateTime) value).toLocalTime());
        }
        return java.sql.Time.valueOf(LocalTime.parse(object2String(value), TIME_FMT));
    }

    public static byte[] toBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return ((String) value).getBytes(StandardCharsets.UTF_8);
        }
        return object2String(value).getBytes(StandardCharsets.UTF_8);
    }

    public static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(value.toString());
        }
        return new BigDecimal(object2String(value));
    }

    private static String formatOracleTimestamp(Object value) {
        try {
            Object ts = value.getClass().getMethod("timestampValue").invoke(value);
            if (ts instanceof java.sql.Timestamp) {
                return DATETIME_FMT.format(((java.sql.Timestamp) ts).toLocalDateTime());
            }
        } catch (Exception ignored) {
            // fall through
        }
        return String.valueOf(value);
    }
}
