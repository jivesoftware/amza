package com.jivesoftware.os.amza.shared;

public interface TimestampedValue<V> {

    long getTimestamp();

    boolean getTombstoned();

    V getValue();
;
}
