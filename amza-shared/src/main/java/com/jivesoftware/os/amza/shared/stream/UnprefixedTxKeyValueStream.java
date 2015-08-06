package com.jivesoftware.os.amza.shared.stream;

public interface UnprefixedTxKeyValueStream {

    boolean row(long rowTxId, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
