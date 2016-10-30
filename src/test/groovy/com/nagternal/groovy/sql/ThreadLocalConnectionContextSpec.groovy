package com.nagternal.groovy.sql

import groovy.sql.Sql
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class ThreadLocalConnectionContextSpec extends Specification {
    Sql sql1 = Mock()
    Sql sql2 = Mock()
    def tx1 = new Transaction(true, sql1)
    def tx2 = new Transaction(false, sql2)

    ExecutorService executor = Executors.newSingleThreadExecutor()

    void setup() {
        0 * _
    }

    void cleanup() {
        executor.shutdown()
    }

    def 'methods work as expected'() {
        given:
            def ctx = new ThreadLocalConnectionContext()
        when:
            ctx.push(tx1)
            ctx.push(tx2)
        then:
            ctx.current() == tx2
        when:
            def result = ctx.pop()
        then:
            result == tx2
            ctx.current() == tx1
        when:
            def second = ctx.pop()
        then:
            second == tx1
            ctx.current() == null
    }

    def 'other thread does not interfere'() {
        given:
            def ctx = new ThreadLocalConnectionContext()
            ctx.push(tx1)
        when:
            def result = executor.submit({ -> ctx.current() } as Callable).get()
        then:
            result == null
            ctx.current() == tx1
        when:
            result = executor.submit({ ->
                ctx.push(tx2)
                ctx.pop()
            } as Callable).get()
        then:
            result == tx2
            ctx.current() == tx1
        cleanup:
            ctx.pop()
    }

    def 'not popping leaves the tx on the thread in the pool'() {
        given:
            def ctx = new ThreadLocalConnectionContext()
        when:
            def result = executor.submit({ -> ctx.current() } as Callable).get()
        then:
            result == null
        when:
            executor.submit({ -> ctx.push(tx2) })
            result = executor.submit({ -> ctx.current() } as Callable).get()
        then:
            result == tx2
        when:
            result = executor.submit({ ->
                ctx.pop()
                ctx.current()
            } as Callable).get()
        then:
            result == null
    }
}
