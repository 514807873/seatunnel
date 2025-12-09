package org.apache.seatunnel.api.common;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class GroupConcatQueryResult implements AutoCloseable {
    private final Connection connection;
    private final Statement statement;
    @Getter
    private final ResultSet resultSet;
    
    public GroupConcatQueryResult(Connection connection, Statement statement, ResultSet resultSet) {
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            log.warn("Failed to close GroupConcatQueryResult resources", e);
        }
    }
}