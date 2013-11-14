package com.jivesoftware.os.amza.shared;

public interface TransactionSetStream<K, V> {

    boolean stream(TransactionSet<K, V> transactionSet) throws Exception;
}
