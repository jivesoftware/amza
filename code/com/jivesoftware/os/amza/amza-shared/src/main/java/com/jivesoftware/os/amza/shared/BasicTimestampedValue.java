package com.jivesoftware.os.amza.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicTimestampedValue<V> implements TimestampedValue<V> {

    private final long timestamp;
    private final boolean tombstoned;
    private final V value;

    @JsonCreator
    public BasicTimestampedValue(
            @JsonProperty("value") V value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("tombstoned") boolean tombstoned) {
        this.timestamp = timestamp;
        this.value = value;
        this.tombstoned = tombstoned;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean getTombstoned() {
        return tombstoned;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "TimestampedValue{" + "timestamp=" + timestamp + ", tombstoned=" + tombstoned + ", value=" + value + '}';
    }
}