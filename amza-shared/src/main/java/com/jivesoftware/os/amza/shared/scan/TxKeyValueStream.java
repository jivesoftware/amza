package com.jivesoftware.os.amza.shared.scan;

public interface TxKeyValueStream {

    boolean row(long rowTxId, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
