package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.delta.DeltaStripeWALStorage;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stream.KeyContainedStream;
import com.jivesoftware.os.amza.shared.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Objects;

/**
 * @author jonathan.colt
 */
public class PartitionStripe {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final PartitionIndex partitionIndex;
    private final DeltaStripeWALStorage storage;
    private final TxPartitionStatus txPartitionStatus;
    private final RowChanges allRowChanges;
    private final PrimaryRowMarshaller<byte[]> primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;
    private final Predicate<VersionedPartitionName> predicate;

    public PartitionStripe(String name,
        PartitionIndex partitionIndex,
        DeltaStripeWALStorage storage,
        TxPartitionStatus txPartitionStatus,
        RowChanges allRowChanges,
        PrimaryRowMarshaller<byte[]> primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller,
        Predicate<VersionedPartitionName> stripingPredicate) {
        this.name = name;
        this.partitionIndex = partitionIndex;
        this.storage = storage;
        this.txPartitionStatus = txPartitionStatus;
        this.allRowChanges = allRowChanges;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.predicate = stripingPredicate;
    }

    public String getName() {
        return name;
    }

    public Object getAwakeCompactionLock() {
        return storage.getAwakeCompactionLock();
    }

    boolean expungePartition(VersionedPartitionName versionedPartitionName) throws Exception {
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
        if (partitionStore != null) {
            return storage.expunge(versionedPartitionName, partitionStore.getWalStorage());
        }
        return false;
    }

    public void highestPartitionTxIds(HighestPartitionTx tx) throws Exception {
        for (VersionedPartitionName versionedPartitionName : Iterables.filter(partitionIndex.getAllPartitions(), predicate)) {
            txPartitionStatus.tx(versionedPartitionName.getPartitionName(),
                (currentVersionedPartitionName, partitionStatus) -> {
                    if (currentVersionedPartitionName.getPartitionVersion() == versionedPartitionName.getPartitionVersion()) {
                        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                        if (partitionStore != null) {
                            long highestTxId = storage.getHighestTxId(versionedPartitionName, partitionStore.getWalStorage());
                            tx.tx(currentVersionedPartitionName, partitionStatus, highestTxId);
                        }
                    }
                    return null;
                });
        }
    }

    public void highestPartitionTxId(PartitionName partitionName, HighestPartitionTx tx) throws Exception {
        txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore != null) {
                    long highestTxId = storage.getHighestTxId(versionedPartitionName, partitionStore.getWalStorage());
                    tx.tx(versionedPartitionName, partitionStatus, highestTxId);
                }
                return null;
            });
    }

    public <R> R txPartition(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        return txPartitionStatus.tx(partitionName, tx);
    }

    public RowsChanged commit(HighwaterStorage highwaterStorage,
        PartitionName partitionName,
        Optional<Long> specificVersion,
        boolean requiresOnline,
        byte[] prefix,
        Commitable updates,
        WALUpdated updated) throws Exception {

        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
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
                    RowsChanged changes = storage.update(highwaterStorage,
                        versionedPartitionName,
                        partitionStatus,
                        partitionStore.getWalStorage(),
                        prefix,
                        updates,
                        updated);
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

    public boolean get(PartitionName partitionName, byte[] prefix, byte[] key, KeyValueStream keyValueStream) throws Exception {
        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    return storage.get(versionedPartitionName, partitionStore.getWalStorage(), prefix, (stream) -> {
                        return stream.stream(key);
                    }, keyValueStream);
                }
            });
    }

    public boolean get(PartitionName partitionName, byte[] prefix, UnprefixedWALKeys keys, KeyValueStream stream) throws Exception {
        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    return storage.get(versionedPartitionName, partitionStore.getWalStorage(), prefix, keys, stream);
                }
            });
    }

    public void rowScan(PartitionName partitionName, KeyValueStream keyValueStream) throws Exception {
        txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    storage.rowScan(versionedPartitionName, partitionStore, keyValueStream);
                }
                return null;
            });
    }

    public void rangeScan(PartitionName partitionName,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueStream keyValueStream) throws Exception {

        txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    storage.rangeScan(versionedPartitionName, partitionStore, fromPrefix, fromKey, toPrefix, toKey, keyValueStream);
                }
                return null;
            });
    }

    public interface TakeRowUpdates<R> {

        R give(VersionedPartitionName versionedPartitionName, TxPartitionStatus.Status partitionStatus, RowStreamer streamer) throws Exception;
    }

    public interface RowStreamer {

        void stream(RowStream rowStream) throws Exception;
    }

    public void takeAllRows(PartitionName partitionName, RowStream rowStream) throws Exception {
        txPartitionStatus.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            if (versionedPartitionName == null || partitionStatus == null) {
                return true;
            }
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                return true;
            } else {
                return storage.takeAllRows(versionedPartitionName, partitionStore.getWalStorage(), rowStream);
            }
        });
    }

    public <R> R takeRowUpdatesSince(PartitionName partitionName,
        long transactionId,
        TakeRowUpdates<R> takeRowUpdates)
        throws Exception {
        return txPartitionStatus.tx(partitionName, (versionedPartitionName, partitionStatus) -> {
            if (versionedPartitionName == null || partitionStatus == null) {
                return takeRowUpdates.give(null, null, null);
            }
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
            if (partitionStore == null) {
                return takeRowUpdates.give(null, null, null);
            } else {
                RowStreamer streamer = (partitionStatus == TxPartitionStatus.Status.ONLINE)
                    ? rowStream -> storage.takeRowsFromTransactionId(versionedPartitionName, partitionStore.getWalStorage(), transactionId, rowStream)
                    : null;
                return takeRowUpdates.give(versionedPartitionName, partitionStatus, streamer);
            }
        });
    }

    public WALHighwater takeFromTransactionId(PartitionName partitionName,
        long transactionId,
        HighwaterStorage highwaterStorage,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream) throws Exception {

        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {

                WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    WALHighwater[] highwater = new WALHighwater[1];
                    primaryRowMarshaller.fromRows(txFpRowStream -> {
                        RowStream stream = (rowFP, rowTxId, rowType, row) -> {
                            if (rowType == RowType.primary) {
                                return txFpRowStream.stream(rowTxId, rowFP, row);
                            } else if (rowType == RowType.highwater) {
                                highwaters.highwater(highwaterRowMarshaller.fromBytes(row));
                            }
                            return true;
                        };
                        if (storage.takeRowsFromTransactionId(versionedPartitionName, partitionStore.getWalStorage(), transactionId, stream)) {
                            highwater[0] = partitionHighwater;
                        }
                        return true;
                    }, txKeyValueStream);
                    return highwater[0];
                }
            });
    }

    public WALHighwater takeFromTransactionId(PartitionName partitionName,
        byte[] prefix,
        long transactionId,
        HighwaterStorage highwaterStorage,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream) throws Exception {

        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {

                WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    WALHighwater[] highwater = new WALHighwater[1];
                    primaryRowMarshaller.fromRows(txFpRowStream -> {
                        RowStream stream = (rowFP, rowTxId, rowType, row) -> {
                            if (rowType == RowType.primary) {
                                return txFpRowStream.stream(rowTxId, rowFP, row);
                            } else if (rowType == RowType.highwater) {
                                highwaters.highwater(highwaterRowMarshaller.fromBytes(row));
                            }
                            return true;
                        };
                        if (storage.takeRowsFromTransactionId(versionedPartitionName, partitionStore.getWalStorage(), prefix, transactionId, stream)) {
                            highwater[0] = partitionHighwater;
                        }
                        return true;
                    }, txKeyValueStream);
                    return highwater[0];
                }
            });
    }

    public long count(PartitionName partitionName) throws Exception {
        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                // any status is OK!
                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    return storage.count(versionedPartitionName, partitionStore.getWalStorage());
                }
            });
    }

    public boolean containsKeys(PartitionName partitionName, byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        return txPartitionStatus.tx(partitionName,
            (versionedPartitionName, partitionStatus) -> {
                Preconditions.checkState(partitionStatus == TxPartitionStatus.Status.ONLINE, "Partition:%s status:%s is not online.", partitionName,
                    partitionStatus);

                PartitionStore partitionStore = partitionIndex.get(versionedPartitionName);
                if (partitionStore == null) {
                    throw new IllegalStateException("No partition defined for " + versionedPartitionName);
                } else {
                    return storage.containsKeys(versionedPartitionName, partitionStore.getWalStorage(), prefix, keys, stream);
                }
            });
    }

    public void load() throws Exception {
        storage.load(txPartitionStatus, partitionIndex, primaryRowMarshaller);
    }

    public boolean mergeable() {
        return storage.mergeable();
    }

    public void merge(boolean force) {
        try {
            storage.merge(partitionIndex, force);
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

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 19 * hash + Objects.hashCode(this.name);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PartitionStripe other = (PartitionStripe) obj;
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        return true;
    }

}
