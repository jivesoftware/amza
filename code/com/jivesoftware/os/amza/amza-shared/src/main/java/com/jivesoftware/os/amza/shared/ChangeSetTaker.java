package com.jivesoftware.os.amza.shared;

public interface ChangeSetTaker {

    <K, V> void take(RingHost ringHost, TableName<K, V> tableName, long transationId, TransactionSetStream transactionSetStream) throws Exception;
}