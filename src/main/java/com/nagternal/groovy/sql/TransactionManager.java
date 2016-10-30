package com.nagternal.groovy.sql;

import groovy.sql.Sql;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class TransactionManager {
    static final Logger LOG = LoggerFactory.getLogger(TransactionManager.class);

    private ConnectionContext connectionContext;
    private DataSource dataSource;

    private final Sql sql;
    private final ReadSql readSql;

    public TransactionManager(DataSource dataSource, ConnectionContext connectionContext) {
        this.dataSource = dataSource;
        this.sql = new Sql(dataSource);
        this.readSql = new ReadSql(dataSource);
        this.connectionContext = connectionContext;
    }

    public static TransactionManager threadLocalTransactionManager(DataSource dataSource) {
        return new TransactionManager(dataSource, new ThreadLocalConnectionContext());
    }

    public boolean transaction() throws SQLException {
        if (connectionContext.current() == null) {
            newTransaction();
            return true;
        }
        return false;
    }

    private void newTransaction() throws SQLException {
        // save auto commit
        Connection connection = connection();
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        connectionContext.push(new Transaction(autoCommit, new Sql(connection)));
    }

    private <T> T doAction(Callable<T> action, boolean isNewTransaction) throws Exception {
        T result = null;
        try {
            result = action.call();
        } catch (Exception ex) {
            try {
                connectionContext.current().getSql().rollback();
            } catch (SQLException rollbackException) {
            } finally {
                throw ex;
            }
        } finally {
            if (isNewTransaction) {
                completeCurrent();
            }
        }
        return result;
    }

    /**
     * Get the instance from current transaction.
     * If not in a transaction then an instance not associated with any transaction.
     * @return an instance
     */
    public Sql getSql() {
        Transaction transaction = connectionContext.current();
        return transaction == null ? sql : transaction.getSql();
    }

    public <T> T inTransaction(Callable<T> action) throws Exception {
        return doAction(action, transaction());
    }

    public <T> T inNewTransaction(Callable<T> action) throws Exception {
        newTransaction();
        return doAction(action, true);
    }

    public <T> T withRead(SqlFunction<T> action) throws Exception {
        Transaction transaction = connectionContext.current();
        Sql sql = transaction != null ? transaction.getSql() : readSql;
        return action.apply(sql);
    }

    public Sql getReadSql() {
        return readSql;
    }

    private void completeCurrent() throws SQLException {
        Sql sql = connectionContext.pop().getSql();
        try {
            sql.commit();
        } catch (SQLException commitException) {
            LOG.info("Exception during commit");
            throw commitException;
        } finally {
            sql.close();
        }
    }

    private Connection connection() throws SQLException {
        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<Connection>() {
                public Connection run() throws SQLException {
                    return dataSource.getConnection();
                }
            });
        } catch (PrivilegedActionException pae) {
            Exception ex = pae.getException();
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            } else {
                throw (RuntimeException) ex;
            }
        }
    }

    public void close() {
        sql.close();
        readSql.close();
    }
}
