package com.jivesoftware.os.amza.shared.stream;

public interface TxFpKeyValueStream {

    boolean stream(long txId,
        long fp,
        byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion,
        byte[] row) throws Exception;

}
