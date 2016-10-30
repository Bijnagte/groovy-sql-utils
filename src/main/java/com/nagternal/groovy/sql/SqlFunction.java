package com.nagternal.groovy.sql;

import groovy.sql.Sql;

@FunctionalInterface
public interface SqlFunction<T> {
    T apply(Sql sql) throws Exception;
}
