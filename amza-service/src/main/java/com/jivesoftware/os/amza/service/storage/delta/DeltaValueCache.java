package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.StripedTLongObjectMap;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class DeltaValueCache {

    private final AmzaStats amzaStats;
    private final long maxValueCacheSizeInBytes;

    private final ConcurrentLinkedQueue<DeltaRow> rowQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong valueCacheSize = new AtomicLong(0);

    public DeltaValueCache(AmzaStats amzaStats, long maxValueCacheSizeInBytes) {
        this.amzaStats = amzaStats;
        this.maxValueCacheSizeInBytes = maxValueCacheSizeInBytes;
    }

    public void put(long fp,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstone,
        WALHighwater highwater,
        StripedTLongObjectMap<DeltaRow> rowMap) {

        long rowSize = rowSize(key, value);
        long cacheSize = valueCacheSize.addAndGet(rowSize);
        while (cacheSize > maxValueCacheSizeInBytes) {
            DeltaRow poll = rowQueue.poll();
            if (poll == null) {
                throw new IllegalStateException("Failed to evict from delta value cache, maths are hard");
            }
            poll.remove();
            long pollSize = rowSize(poll.key, poll.value);
            cacheSize = valueCacheSize.addAndGet(-pollSize);
            amzaStats.deltaValueCacheRemoves.incrementAndGet();
        }
        amzaStats.deltaValueCacheUtilization.set((double) cacheSize / (double) maxValueCacheSizeInBytes);

        DeltaRow deltaRow = new DeltaRow(fp, key, value, valueTimestamp, valueTombstone, highwater, new WeakReference<>(rowMap));
        rowMap.put(fp, deltaRow);
        rowQueue.add(deltaRow);
        amzaStats.deltaValueCacheAdds.incrementAndGet();
    }

    private long rowSize(byte[] key, byte[] value) {
        return (long) (key.length) + (long) (value != null ? value.length : 0);
    }

    public DeltaRow get(long fp, StripedTLongObjectMap<DeltaRow> rowMap) {
        DeltaRow got = rowMap.get(fp);
        if (got == null) {
            amzaStats.deltaValueCacheMisses.incrementAndGet();
        } else {
            amzaStats.deltaValueCacheHits.incrementAndGet();
        }
        return got;
    }

    public static class DeltaRow {

        public final long fp;
        public final byte[] key;
        public final byte[] value;
        public final long valueTimestamp;
        public final boolean valueTombstone;
        public final WALHighwater highwater;
        public final WeakReference<StripedTLongObjectMap<DeltaRow>> rowMapRef;

        public DeltaRow(long fp, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstone, WALHighwater highwater,
            WeakReference<StripedTLongObjectMap<DeltaRow>> rowMapRef) {
            this.fp = fp;
            this.key = key;
            this.value = value;
            this.valueTimestamp = valueTimestamp;
            this.valueTombstone = valueTombstone;
            this.highwater = highwater;
            this.rowMapRef = rowMapRef;
        }

        public void remove() {
            StripedTLongObjectMap<DeltaRow> rowMap = rowMapRef.get();
            if (rowMap != null) {
                rowMap.remove(fp);
            }
        }
    }
}
