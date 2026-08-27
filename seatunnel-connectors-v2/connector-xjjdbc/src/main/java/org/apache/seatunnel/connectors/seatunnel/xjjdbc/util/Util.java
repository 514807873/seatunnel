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
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Properties;

/** Small helpers shared by the XjJdbc sink: connection creation and value-to-string fallback. */
public final class Util {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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

    /** Fallback conversion used when a typed PreparedStatement binding is not accepted. */
    public static String object2String(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof java.util.Date) {
            return DATETIME_FMT.format(((java.util.Date) value).toInstant());
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value).format(DATE_FMT);
        }
        if (value instanceof LocalDateTime) {
            return DATETIME_FMT.format((LocalDateTime) value);
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).stripTrailingZeros().toPlainString();
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        return String.valueOf(value);
    }
}
