package com.nagternal.groovy.sql;

public interface ConnectionContext {

    void push(Transaction sql);

    Transaction pop();

    Transaction current();
}
