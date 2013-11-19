package com.jivesoftware.os.amza.shared;

public interface HighWaterMarks {

    void clearRing(RingHost ringHost);

    void set(RingHost ringHost, TableName tableName, long highWatermark);

    void clear(RingHost ringHost, TableName tableName);

    Long get(RingHost ringHost, TableName tableName);
}
