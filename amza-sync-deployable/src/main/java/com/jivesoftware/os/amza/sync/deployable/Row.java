package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class Row {

    public final byte[] prefix;
    public final byte[] key;
    public final byte[] value;
    public final long valueTimestamp;
    public final boolean valueTombstoned;

    @JsonCreator
    public Row(@JsonProperty("prefix") byte[] prefix,
        @JsonProperty("key") byte[] key,
        @JsonProperty("value") byte[] value,
        @JsonProperty("valueTimestamp") long valueTimestamp,
        @JsonProperty("valueTombstoned") boolean valueTombstoned) {
        this.prefix = prefix;
        this.key = key;
        this.value = value;
        this.valueTimestamp = valueTimestamp;
        this.valueTombstoned = valueTombstoned;
    }
}
