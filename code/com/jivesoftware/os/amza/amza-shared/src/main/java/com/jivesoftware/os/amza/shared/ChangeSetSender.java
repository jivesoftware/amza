package com.jivesoftware.os.amza.shared;

import java.util.NavigableMap;

public interface ChangeSetSender {

    <K, V> void sendChangeSet(RingHost ringHost, TableName<K, V> mapName,
            NavigableMap<K, TimestampedValue<V>> changes) throws Exception;
}
