package com.jivesoftware.os.amza.shared.wal;

/**
 *
 */
public class WALIndexable {

    public final long txId;
    public final byte[] key;
    public final long valueTimestamp;
    public final boolean valueTombstoned;
    public final long fp;

    public WALIndexable(long txId, byte[] key, long valueTimestamp, boolean valueTombstoned, long fp) {
        this.txId = txId;
        this.key = key;
        this.valueTimestamp = valueTimestamp;
        this.valueTombstoned = valueTombstoned;
        this.fp = fp;
    }
}
