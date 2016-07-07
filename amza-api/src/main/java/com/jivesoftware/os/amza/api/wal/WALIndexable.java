package com.jivesoftware.os.amza.api.wal;

/**
 *
 */
public class WALIndexable {

    public final long txId;
    public final byte[] prefix;
    public final byte[] key;
    public final byte[] value;
    public final long valueTimestamp;
    public final boolean valueTombstoned;
    public final long valueVersion;
    public final long fp;

    public WALIndexable(long txId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion, long fp) {
        this.txId = txId;
        this.prefix = prefix;
        this.key = key;
        this.valueTimestamp = valueTimestamp;
        this.valueTombstoned = valueTombstoned;
        this.valueVersion = valueVersion;
        this.fp = fp;
        this.value = value;
    }
}
