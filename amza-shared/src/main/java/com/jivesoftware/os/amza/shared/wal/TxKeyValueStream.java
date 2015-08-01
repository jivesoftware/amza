package com.jivesoftware.os.amza.shared.wal;

public interface TxKeyValueStream {

    boolean row(long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
