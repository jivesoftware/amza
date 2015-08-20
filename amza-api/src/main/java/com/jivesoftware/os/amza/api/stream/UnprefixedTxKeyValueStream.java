package com.jivesoftware.os.amza.api.stream;

public interface UnprefixedTxKeyValueStream {

    boolean row(long rowTxId, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;

}
