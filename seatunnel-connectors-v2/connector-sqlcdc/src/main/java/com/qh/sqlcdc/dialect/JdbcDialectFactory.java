package com.qh.sqlcdc.dialect;

import com.qh.sqlcdc.dialect.ClickHouse.ClickHouseDialect;
import com.qh.sqlcdc.dialect.mysql.MysqlDialect;
import com.qh.sqlcdc.dialect.oracle.OracleDialect;
import com.qh.sqlcdc.dialect.pgsql.PgSqlDialect;
import com.qh.sqlcdc.dialect.sqlserver.SqlServerDialect;

import java.util.HashMap;

public class JdbcDialectFactory {

    private static final HashMap<String, JdbcDialect> map =
            new HashMap<String, JdbcDialect>() {
                {
                    put("oracle", new OracleDialect());
                    put("mysql", new MysqlDialect());
                    put("clickhouse", new ClickHouseDialect());
                    put("pgsql", new PgSqlDialect());
                    put("sqlserver", new SqlServerDialect());
                }
            };

    public static JdbcDialect getJdbcDialect(String param) {
        String lowerCase = param.toLowerCase();
        return map.get(lowerCase);
    }
}
