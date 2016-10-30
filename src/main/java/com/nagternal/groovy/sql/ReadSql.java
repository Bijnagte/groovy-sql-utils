package com.nagternal.groovy.sql;

import groovy.sql.Sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class ReadSql extends Sql {
    public ReadSql(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected Connection createConnection() throws SQLException {
        Connection connection =  super.createConnection();
        connection.setReadOnly(true);
        return connection;
    }
}
