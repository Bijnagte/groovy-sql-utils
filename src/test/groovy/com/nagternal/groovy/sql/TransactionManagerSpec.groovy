package com.nagternal.groovy.sql

import groovy.sql.Sql
import org.h2.jdbcx.JdbcConnectionPool
import org.h2.tools.Server
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class TransactionManagerSpec extends Specification {

    ExecutorService executor = Executors.newSingleThreadExecutor()

    Server db
    JdbcConnectionPool ds
    TransactionManager tx

    void setup() {
        db = Server.createTcpServer('-baseDir', './build/db').start()
        ds = JdbcConnectionPool.create("jdbc:h2:./build/db/test", "sa", "sa")
        new Sql(ds).execute('CREATE TABLE test (a VARCHAR(20))')
        tx = TransactionManager.threadLocalTransactionManager(ds)
    }

    void cleanup() {
        tx.close()
        ds.dispose()
        db.shutdown()
        new File('./build/db').eachFile { it.delete() }
        executor.shutdown()
    }

    def 'insert in transaction'() {
        when:
            def inTx = tx.inTransaction {
                tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'success')
                tx.sql.firstRow('SELECT a FROM test').get('a')
            }
        then:
            tx.sql.firstRow('SELECT a FROM test').get('a') == 'success'
            inTx == 'success'
    }

    def 'roll back transaction'() {
        when:
            def beforeRollBack
            tx.inTransaction {
                tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'success')
                beforeRollBack = tx.sql.rows('SELECT a FROM test')
                throw new Exception('oops')
            }

        then: 'the insert was visible in the transaction'
            Exception ex = thrown()
            ex.message == 'oops'
            beforeRollBack.size() == 1
        and: 'it was rolled back'
            tx.sql.rows('SELECT a FROM test').size() == 0
    }

    def 'roll back nested transaction calls'() {
        when:
            def duringFirst
            def duringSecond
            tx.inTransaction {
                tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'first')
                tx.inTransaction {
                    tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'second')
                    duringSecond = tx.sql.rows('SELECT a FROM test')
                }
                duringFirst = tx.sql.rows('SELECT a FROM test')
                throw new Exception('oops')
            }

        then: 'the insert was visible in the transaction'
            Exception ex = thrown()
            ex.message == 'oops'
        and: 'both calls can read each others inserts'
            duringFirst*.get('a') == ['first', 'second']
            duringSecond*.get('a') == ['first', 'second']
        and: 'it was rolled back'
            tx.sql.rows('SELECT a FROM test').size() == 0
    }

    def 'in new transaction is independent'() {
        when:
            def duringTx1
            def duringTx2
            tx.inTransaction {
                tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'first')
                tx.inNewTransaction {
                    tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'second')
                    duringTx2 = tx.sql.rows('SELECT a FROM test')
                }
                duringTx1 = tx.sql.rows('SELECT a FROM test')
                throw new Exception('oops')
            }
        then:
            Exception ex = thrown()
            ex.message == 'oops'
            duringTx1*.get('a') == ['first', 'second']
            duringTx2*.get('a') == ['second']
        and: 'the inner transaction did not roll back'
            tx.sql.rows('SELECT a FROM test')*.get('a') == ['second']
    }

    def 'read from outside transaction'() {
        given:
            CountDownLatch latch = new CountDownLatch(1)
        when:
            def inTx = executor.submit({ ->
                tx.inTransaction {
                    tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'success')
                    latch.await()
                    tx.sql.rows('SELECT a FROM test')
                }
            } as Callable)

            def outTx = tx.sql.rows('SELECT a FROM test')

            latch.countDown()
        then:
            inTx.get().size() == 1
            outTx.size() == 0
        expect:
            tx.sql.firstRow('SELECT a FROM test').get('a') == 'success'
    }

    def 'read sql does not participate in transaction'() {
        when:
            def readResult
            tx.inTransaction {
                tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'success')
                readResult = tx.readSql.rows('SELECT a FROM test')
            }
        then: 'the insert was not visible to the read sql in the transaction'
            readResult.size() == 0
        and: 'it is visible after the transaction'
            tx.readSql.rows('SELECT a FROM test').size() == 1
    }

    def 'with read sql uses the transaction connection if available'() {
        expect:
            tx.withRead { sql -> sql instanceof ReadSql } == true
        when:
            def readResult
            tx.inTransaction {
                tx.sql.executeUpdate('INSERT INTO test (a) VALUES (?)', 'success')
                readResult = tx.withRead { sql ->
                    readResult = sql.rows('SELECT a FROM test')
                }
            }
        then: 'the insert was visible in the transaction'
            readResult.size() == 1
    }
}
