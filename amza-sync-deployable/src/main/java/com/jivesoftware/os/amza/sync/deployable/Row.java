package com.jivesoftware.os.amza.sync.deployable;

/**
 *
 */
public class Row {

    public final byte[] prefix;
    public final byte[] key;
    public final byte[] value;
    public final long valueTimestamp;
    public final boolean valueTombstoned;

    public Row(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) {
        this.prefix = prefix;
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
        this.valueTombstoned = valueTombstoned;
    }
}
