package com.nagternal.groovy.sql;

import groovy.sql.Sql;

public class Transaction {
    private final boolean autoCommit;
    private final Sql sql; // TODO: wrap in one that throws unsupported operation

    public Transaction(boolean autoCommit, Sql sql) {
        this.autoCommit = autoCommit;
        this.sql = sql;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public Sql getSql() {
        return sql;
    }
}
