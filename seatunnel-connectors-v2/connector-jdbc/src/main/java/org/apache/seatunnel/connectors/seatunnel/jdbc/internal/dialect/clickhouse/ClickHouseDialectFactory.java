package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.clickhouse;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectFactory;

import com.google.auto.service.AutoService;

@AutoService(JdbcDialectFactory.class)
public class ClickHouseDialectFactory implements JdbcDialectFactory {
    @Override
    public String dialectFactoryName() {
        return DatabaseIdentifier.CLICKHOUSE;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:clickhouse:");
    }

    @Override
    public JdbcDialect create() {
        return new ClickHouseDialect();
    }
}
