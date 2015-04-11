package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.service.storage.RowsStorageUpdates;
import com.jivesoftware.os.amza.service.storage.delta.StripeWALStorage;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class RegionStripe {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaStats amzaStats;
    private final OrderIdProvider idProvider;
    private final RegionIndex regionIndex;
    private final StripeWALStorage storage;
    private final RowChanges allRowChanges;
    private final Predicate<RegionName> predicate;

    public RegionStripe(AmzaStats amzaStats,
        OrderIdProvider idProvider,
        RegionIndex regionIndex,
        StripeWALStorage storage,
        RowChanges allRowChanges,
        Predicate<RegionName> stripingPredicate
    ) {
        this.amzaStats = amzaStats;
        this.idProvider = idProvider;
        this.regionIndex = regionIndex;
        this.storage = storage;
        this.allRowChanges = allRowChanges;
        this.predicate = stripingPredicate;
    }

    public Iterable<RegionName> getActiveRegions() {
        return Iterables.filter(regionIndex.getActiveRegions(), predicate);
    }

    public RowsChanged commit(RegionName regionName,
        WALReplicator replicator,
        WALStorageUpdateMode walStorageUpdateMode,
        Scannable<WALValue> updates) throws Exception {

        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            RowsChanged changes = storage.update(regionName, regionStore.getWalStorage(), replicator, walStorageUpdateMode, updates);
            if (allRowChanges != null && !changes.isEmpty()) {
                allRowChanges.changes(changes);
            }
            return changes;
        }
    }

    public void flush(boolean fsync) throws Exception {
        storage.flush(fsync);
    }

    public RowStoreUpdates startTransaction(RegionName regionName, long nextId) {
        return new RowStoreUpdates(amzaStats, regionName, this, new RowsStorageUpdates(regionName, this, idProvider.nextId()));
    }

    public WALValue get(RegionName regionName, WALKey key) throws Exception {
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            return storage.get(regionName, regionStore.getWalStorage(), key);
        }
    }

    public void rowScan(RegionName regionName, Scan<WALValue> scan) throws Exception {
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            storage.rowScan(regionName, regionStore, scan);
        }
    }

    public void rangeScan(RegionName regionName, WALKey from, WALKey to, Scan<WALValue> stream) throws Exception {
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            storage.rangeScan(regionName, regionStore, from, to, stream);
        }
    }

    public void takeRowUpdatesSince(RegionName regionName, long transactionId, RowStream rowStream) throws Exception {
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            storage.takeRowUpdatesSince(regionName, regionStore.getWalStorage(), transactionId, rowStream);
        }
    }

    public long count(RegionName regionName) throws Exception {
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            return storage.count(regionName, regionStore.getWalStorage());
        }
    }

    public boolean containsKey(RegionName regionName, WALKey key) throws Exception {
        RegionStore regionStore = regionIndex.get(regionName);
        if (regionStore == null) {
            throw new IllegalStateException("No region defined for" + regionName);
        } else {
            return storage.containsKey(regionName, regionStore.getWalStorage(), key);
        }
    }

    public void load(ScheduledExecutorService compactDeltaWALThread) throws Exception {
        storage.load(regionIndex);

        compactDeltaWALThread.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    storage.compact(regionIndex);
                } catch (Throwable x) {
                    LOG.error("Compactor failed.", x);
                }
            }
        }, 1, 1, TimeUnit.MINUTES); // TODO expose to config

    }

}
