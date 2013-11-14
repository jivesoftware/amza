package com.jivesoftware.os.amza.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TimestampedValue<V> {

    private final long timestamp;
    private final boolean tombstoned;
    private final V value;

    @JsonCreator
    public TimestampedValue(
            @JsonProperty("value") V value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("tombstoned") boolean tombstoned) {
        this.timestamp = timestamp;
        this.value = value;
        this.tombstoned = tombstoned;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean getTombstoned() {
        return tombstoned;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "TimestampedValue{" + "timestamp=" + timestamp + ", tombstoned=" + tombstoned + ", value=" + value + '}';
    }
}