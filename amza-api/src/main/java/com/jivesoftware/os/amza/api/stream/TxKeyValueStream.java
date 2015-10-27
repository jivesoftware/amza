package com.jivesoftware.os.amza.api.stream;

public interface TxKeyValueStream {

    boolean stream(long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) throws Exception;

}
