package com.nagternal.groovy.sql;

import java.util.ArrayDeque;
import java.util.Deque;

public class ThreadLocalConnectionContext implements ConnectionContext {
    private final ThreadLocal<Deque<Transaction>> connectionStack =
            ThreadLocal.withInitial(ArrayDeque::new);

    @Override
    public void push(Transaction transaction) {
        connectionStack.get().push(transaction);
    }

    @Override
    public Transaction pop() {
        return connectionStack.get().pop();
    }

    @Override
    public Transaction current() {
        return connectionStack.get().peek();
    }
}
