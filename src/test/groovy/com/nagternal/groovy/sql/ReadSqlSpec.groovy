package com.nagternal.groovy.sql

import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection


class ReadSqlSpec extends Specification {

    DataSource dataSource = Mock()
    Connection connection = Mock()

    def 'create connection is read only'() {
        when:
            new ReadSql(dataSource).createConnection()
        then:
            1 * dataSource.connection >> connection
            1 * connection.setReadOnly(true)
        0 * _
    }
}
