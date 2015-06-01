package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.service.storage.RowsStorageUpdates;
import com.jivesoftware.os.amza.service.storage.delta.StripeWALStorage;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.Highwaters;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionTx;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.TxRegionStatus;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class RegionStripe {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final AmzaStats amzaStats;
    private final RegionIndex regionIndex;
    private final StripeWALStorage storage;
    private final TxRegionStatus txRegionState;
    private final RowChanges allRowChanges;
    private final Predicate<VersionedRegionName> predicate;

    public RegionStripe(String name,
        AmzaStats amzaStats,
        RegionIndex regionIndex,
        StripeWALStorage storage,
        TxRegionStatus txRegionState,
        RowChanges allRowChanges,
        Predicate<VersionedRegionName> stripingPredicate) {
        this.name = name;
        this.amzaStats = amzaStats;
        this.regionIndex = regionIndex;
        this.storage = storage;
        this.txRegionState = txRegionState;
        this.allRowChanges = allRowChanges;
        this.predicate = stripingPredicate;
    }

    boolean expungeRegion(VersionedRegionName versionedRegionName) throws Exception {
        RegionStore regionStore = regionIndex.get(versionedRegionName);
        if (regionStore != null) {
            return storage.expunge(versionedRegionName, regionStore.getWalStorage());
        }
        return false;
    }

    public void txAllRegions(RegionTx<Void> tx) throws Exception {
        for (VersionedRegionName versionedRegionName : Iterables.filter(regionIndex.getAllRegions(), predicate)) {
            txRegionState.tx(versionedRegionName.getRegionName(), (currentVersionedRegionName, regionStatus) -> {
                if (currentVersionedRegionName.getRegionVersion() == versionedRegionName.getRegionVersion()) {
                    return tx.tx(currentVersionedRegionName, regionStatus);
                }
                return null;
            });
        }
    }

    public RowsChanged commit(RegionName regionName,
        Optional<Long> specificVersion,
        WALReplicator replicator,
        WALStorageUpdateMode walStorageUpdateMode,
        Commitable<WALValue> updates) throws Exception {

        return txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);
            if (specificVersion.isPresent() && versionedRegionName.getRegionVersion() != specificVersion.get()) {
                return null;
            }
            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + regionName);
            } else {
                RowsChanged changes = storage.update(versionedRegionName, regionStore.getWalStorage(), replicator,
                    walStorageUpdateMode, updates);
                if (allRowChanges != null && !changes.isEmpty()) {
                    allRowChanges.changes(changes);
                }
                return changes;
            }
        });
    }

    public void flush(boolean fsync) throws Exception {
        storage.flush(fsync);
    }

    public RowStoreUpdates startTransaction(RegionName regionName) {
        return new RowStoreUpdates(amzaStats, regionName, this, new RowsStorageUpdates(regionName, this));
    }

    public WALValue get(RegionName regionName, WALKey key) throws Exception {
        return txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                return storage.get(versionedRegionName, regionStore.getWalStorage(), key);
            }
        });
    }

    public void rowScan(RegionName regionName, Scan<WALValue> scan) throws Exception {
        txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                storage.rowScan(versionedRegionName, regionStore, scan);
            }
            return null;
        });
    }

    public void rangeScan(RegionName regionName, WALKey from, WALKey to, Scan<WALValue> stream) throws Exception {
        txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                storage.rangeScan(versionedRegionName, regionStore, from, to, stream);
            }
            return null;
        });
    }

    public void takeRowUpdatesSince(RegionName regionName, long transactionId, RowStream rowStream) throws Exception {
        txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                storage.takeRowUpdatesSince(versionedRegionName, regionStore.getWalStorage(), transactionId, rowStream);
            }
            return null;
        });
    }

    public WALHighwater takeFromTransactionId(RegionName regionName,
        long transactionId,
        HighwaterStorage highwaterStorage,
        Highwaters highwaters,
        Scan<WALValue> scan) throws Exception {

        return txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {

            WALHighwater regionHighwater = highwaterStorage.getRegionHighwater(versionedRegionName);
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                if (storage.takeFromTransactionId(versionedRegionName, regionStore.getWalStorage(), transactionId, highwaters, scan)) {
                    return regionHighwater;
                } else {
                    return null;
                }
            }
        });
    }

    public long count(RegionName regionName) throws Exception {
        return txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                return storage.count(versionedRegionName, regionStore.getWalStorage());
            }
        });
    }

    public boolean containsKey(RegionName regionName, WALKey key) throws Exception {
        return txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {
            Preconditions.checkState(regionStatus == TxRegionStatus.Status.ONLINE, "Region:%s status:%s is not online.", regionName, regionStatus);

            RegionStore regionStore = regionIndex.get(versionedRegionName);
            if (regionStore == null) {
                throw new IllegalStateException("No region defined for " + versionedRegionName);
            } else {
                return storage.containsKey(versionedRegionName, regionStore.getWalStorage(), key);
            }
        });
    }

    public void load() throws Exception {
        storage.load(regionIndex);
    }

    public void compact() {
        try {
            storage.compact(regionIndex);
        } catch (Throwable x) {
            LOG.error("Compactor failed.", x);
        }
    }

    @Override
    public String toString() {
        return "RegionStripe{"
            + "name='" + name + '\''
            + '}';
    }

}
