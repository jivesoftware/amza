package com.jivesoftware.os.amza.shared;

import java.util.List;

public interface AmzaInstance {

    <K, V> void changes(TableName<K, V> tableName, TableDelta<K, V> changes) throws Exception;

    <K, V> void takeTableChanges(TableName<K, V> tableName, long transationId, TransactionSetStream<K, V> transactionSetStream) throws Exception;

    void addRingHost(String ringName, RingHost ringHost) throws Exception;

    void removeRingHost(String ringName, RingHost ringHost) throws Exception;

    List<RingHost> getRing(String ringName) throws Exception;
}
