package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.shared.HighWaterMarks;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryBackedHighWaterMarks implements HighWaterMarks {

    private final ConcurrentHashMap<RingHost, ConcurrentHashMap<TableName, Long>> lastTransactionIds = new ConcurrentHashMap<>();

    @Override
    public void clearRing(RingHost ringHost) {
        lastTransactionIds.remove(ringHost);
    }

    @Override
    public void set(RingHost ringHost, TableName tableName, long highWatermark) {
        ConcurrentHashMap<TableName, Long> lastTableTransactionIds = lastTransactionIds.get(ringHost);
        if (lastTableTransactionIds == null) {
            lastTableTransactionIds = new ConcurrentHashMap<>();
            lastTransactionIds.put(ringHost, lastTableTransactionIds);
        }
        lastTableTransactionIds.put(tableName, highWatermark);
    }

    @Override
    public void clear(RingHost ringHost, TableName tableName) {
        ConcurrentHashMap<TableName, Long> lastTableTransactionIds = lastTransactionIds.get(ringHost);
        if (lastTableTransactionIds != null) {
            lastTableTransactionIds.remove(tableName);
            //if (lastTableTransactionIds.isEmpty()) { // TODO do this is a thread safe way
            //       lastTransactionIds.remove(ringHost);
            //}
        }
    }

    @Override
    public Long get(RingHost ringHost, TableName tableName) {
        ConcurrentHashMap<TableName, Long> lastTableTransactionIds = lastTransactionIds.get(ringHost);
        if (lastTableTransactionIds == null) {
            return -1L;
        }
        Long got = lastTableTransactionIds.get(tableName);
        if (got == null) {
            return -1L;
        }
        return got;
    }
}
