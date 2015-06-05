package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.delta.StripeWALStorage;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionTx;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.Highwaters;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
public class PartitionStripe {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final StripeWALStorage storage;
    private final TxPartitionStatus txPartitionState;
    private final RowChanges allRowChanges;
    private final Predicate<VersionedPartitionName> predicate;

    public PartitionStripe(String name,
        AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        StripeWALStorage storage,
        TxPartitionStatus txPartitionState,
        RowChanges allRowChanges,
        Predicate<VersionedPartitionName> stripingPredicate) {
        this.name = name;
        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.storage = storage;
        this.txPartitionState = txPartitionState;
        this.allRowChanges = allRowChanges;
        this.predicate = stripingPredicate;
    }

    boolean expungePartition(VersionedPartitionName versionedPartitionName) throws Exception {
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore != null) {
            return storage.expunge(versionedPartitionName, partitionStore.getWalStorage());
        }
        return false;
    }

    public void txAllPartitions(PartitionTx<Void> tx) throws Exception {
        for (VersionedPartitionName versionedPartitionName : Iterables.filter(partitionIndex.getAllPartitions(), predicate)) {
            txPartitionState.tx(versionedPartitionName.getPartitionName(), (currentVersionedPartitionName, partitionStatus) -> {
                if (currentVersionedPartitionName.getPartitionVersion() == versionedPartitionName.getPartitionVersion()) {
                    return tx.tx(currentVersionedPartitionName, partitionStatus);
                }
                return null;
            });
        }
    }

    public RowsChanged commit(PartitionName partitionName,
        Optional<Long> specificVersion,
        boolean requiresOnline,
        Commitable<WALValue> updates) throws Exception {

        return txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE || !requiresOnline, "Partition:%s status:%s is not online.",
                partitionName,
                partitionStatus);
            if (specificVersion.isPresent() && versionedPartitionName.getPartitionVersion() != specificVersion.get()) {
                return null;
            }
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + partitionName);
            } else {
                RowsChanged changes = storage.update(versionedPartitionName, partitionStore.getWalStorage(), updates);
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

    public WALValue get(PartitionName partitionName, WALKey key) throws Exception {
        return txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                return storage.get(versionedPartitionName, partitionStore.getWalStorage(), key);
            }
        });
    }

    public void rowScan(PartitionName partitionName, Scan<WALValue> scan) throws Exception {
        txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                storage.rowScan(versionedPartitionName, partitionStore, scan);
            }
            return null;
        });
    }

    public void rangeScan(PartitionName partitionName, WALKey from, WALKey to, Scan<WALValue> stream) throws Exception {
        txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                storage.rangeScan(versionedPartitionName, partitionStore, from, to, stream);
            }
            return null;
        });
    }

    public void takeRowUpdatesSince(PartitionName partitionName, long transactionId, RowStream rowStream) throws Exception {
        txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                storage.takeRowUpdatesSince(versionedPartitionName, partitionStore.getWalStorage(), transactionId, rowStream);
            }
            return null;
        });
    }

    public WALHighwater takeFromTransactionId(PartitionName partitionName,
        long transactionId,
        HighwaterStorage highwaterStorage,
        Highwaters highwaters,
        Scan<WALValue> scan) throws Exception {

        return txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {

            WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                if (storage.takeFromTransactionId(versionedPartitionName, partitionStore.getWalStorage(), transactionId, highwaters, scan)) {
                    return partitionHighwater;
                } else {
                    return null;
                }
            }
        });
    }

    public long count(PartitionName partitionName) throws Exception {
        return txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                return storage.count(versionedPartitionName, partitionStore.getWalStorage());
            }
        });
    }

    public boolean containsKey(PartitionName partitionName, WALKey key) throws Exception {
        return txPartitionState.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                partitionStatus);

            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                throw new IllegalStateException("No partition defined for " + versionedPartitionName);
            } else {
                return storage.containsKey(versionedPartitionName, partitionStore.getWalStorage(), key);
            }
        });
    }

    public void load() throws Exception {
        storage.load(partitionIndex);
    }

    public void compact() {
        try {
            storage.compact(partitionIndex);
        } catch (Throwable x) {
            LOG.error("Compactor failed.", x);
        }
    }

    @Override
    public String toString() {
        return "PartitionStripe{"
            + "name='" + name + '\''
            + '}';
    }

}
