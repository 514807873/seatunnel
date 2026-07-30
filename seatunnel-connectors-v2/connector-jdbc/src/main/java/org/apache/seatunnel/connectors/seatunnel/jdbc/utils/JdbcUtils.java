package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;

public class JdbcUtils {
    public static Driver getDriver(String file, String className) throws MalformedURLException {
        Driver driver = null;
        try {
            String jarFilePath = String.format("jar:file:%s!/", file);
            ClassLoader parent = CatalogUtils.class.getClassLoader().getParent();
            URLClassLoader loader = new URLClassLoader(new URL[] {new URL(jarFilePath)}, parent);
            driver = (Driver) Class.forName(className, true, loader).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return driver;
    }
}
