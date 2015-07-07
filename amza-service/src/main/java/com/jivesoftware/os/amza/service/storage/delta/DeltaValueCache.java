package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.service.storage.delta.DeltaWAL.KeyValueHighwater;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class DeltaValueCache {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final long maxValueCacheSizeInBytes;

    private final ConcurrentLinkedQueue<DeltaRow> rowQueue = new ConcurrentLinkedQueue<>();
    private final AtomicLong valueCacheSize = new AtomicLong(0);

    public DeltaValueCache(AmzaStats amzaStats, long maxValueCacheSizeInBytes) {
        this.amzaStats = amzaStats;
        this.maxValueCacheSizeInBytes = maxValueCacheSizeInBytes;
    }

    public void put(long fp, KeyValueHighwater keyValueHighwater, ConcurrentHashMap<Long, DeltaRow> rowMap) {
        WALKey key = keyValueHighwater.key;
        WALValue value = keyValueHighwater.value;
        long rowSize = rowSize(key, value);
        long cacheSize = valueCacheSize.addAndGet(rowSize);
        while (cacheSize > maxValueCacheSizeInBytes) {
            DeltaRow poll = rowQueue.poll();
            if (poll == null) {
                throw new IllegalStateException("Failed to evict from delta value cache, maths are hard");
            }
            poll.remove();
            long pollSize = rowSize(poll.keyValueHighwater.key, poll.keyValueHighwater.value);
            cacheSize = valueCacheSize.addAndGet(-pollSize);
            amzaStats.deltaValueCacheRemoves.incrementAndGet();
        }
        amzaStats.deltaValueCacheUtilization.set((double) cacheSize / (double) maxValueCacheSizeInBytes);

        DeltaRow deltaRow = new DeltaRow(fp, keyValueHighwater, rowMap);
        rowMap.put(fp, deltaRow);
        rowQueue.add(deltaRow);
        amzaStats.deltaValueCacheAdds.incrementAndGet();
    }

    private long rowSize(WALKey key, WALValue value) {
        return (long) (key.getKey().length) + (long) (value.getValue() != null ? value.getValue().length : 0);
    }

    public DeltaRow get(long fp, ConcurrentHashMap<Long, DeltaRow> rowMap) {
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
        public final KeyValueHighwater keyValueHighwater;
        public final WeakReference<ConcurrentHashMap<Long, DeltaRow>> rowMapRef;

        public DeltaRow(long fp,
            KeyValueHighwater keyValueHighwater,
            ConcurrentHashMap<Long, DeltaRow> rowMap) {
            this.fp = fp;
            this.keyValueHighwater = keyValueHighwater;
            this.rowMapRef = new WeakReference<>(rowMap);
        }

        public void remove() {
            ConcurrentHashMap<Long, DeltaRow> rowMap = rowMapRef.get();
            if (rowMap != null) {
                rowMap.remove(fp);
            }
        }
    }
}
