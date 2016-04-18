package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValueStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.storage.HighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.service.storage.delta.DeltaStripeWALStorage;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Objects;

/**
 * @author jonathan.colt
 */
public class PartitionStripe {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String name;
    private final int stripe;
    private final PartitionIndex partitionIndex;
    private final StorageVersionProvider storageVersionProvider;
    private final DeltaStripeWALStorage storage;
    private final RowChanges allRowChanges;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final HighwaterRowMarshaller<byte[]> highwaterRowMarshaller;

    public PartitionStripe(String name,
        int stripe,
        PartitionIndex partitionIndex,
        StorageVersionProvider storageVersionProvider,
        DeltaStripeWALStorage storage,
        RowChanges allRowChanges,
        PrimaryRowMarshaller primaryRowMarshaller,
        HighwaterRowMarshaller<byte[]> highwaterRowMarshaller) {
        this.name = name;
        this.stripe = stripe;
        this.partitionIndex = partitionIndex;
        this.storageVersionProvider = storageVersionProvider;
        this.storage = storage;
        this.allRowChanges = allRowChanges;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
    }

    public String getName() {
        return name;
    }

    public Object getAwakeCompactionLock() {
        return storage.getAwakeCompactionLock();
    }

    void deleteDelta(VersionedPartitionName versionedPartitionName) throws Exception {
        storage.delete(versionedPartitionName);
    }

    boolean exists(VersionedPartitionName localVersionedPartitionName) throws Exception {
        return partitionIndex.exists(localVersionedPartitionName, stripe);
    }

    public void load(TxPartitionState txPartitionState) throws Exception {
        storage.load(txPartitionState, partitionIndex, stripe, storageVersionProvider, primaryRowMarshaller);
    }

    public long highestAquariumTxId(VersionedAquarium versionedAquarium, HighestPartitionTx tx) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore != null) {
            long highestTxId = storage.getHighestTxId(versionedPartitionName, partitionStore.getWalStorage());
            return tx.tx(versionedAquarium, highestTxId);
        } else {
            return tx.tx(null, -1);
        }
    }

    public interface PartitionLeadershipToken {

        long getLeadershipToken(VersionedAquarium versionedAquarium) throws Exception;
    }

    public interface PostCommit {

        void committed(VersionedPartitionName versionedPartitionName, long leadershipToken, long largestCommittedTxId) throws Exception;
    }

    public RowsChanged commit(HighwaterStorage highwaterStorage,
        VersionedAquarium versionedAquarium,
        boolean directApply,
        Optional<Long> specificVersion,
        boolean requiresOnline,
        byte[] prefix,
        PartitionLeadershipToken partitionLeadershipToken,
        Commitable updates,
        PostCommit postCommit,
        WALUpdated updated) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        long leadershipToken = partitionLeadershipToken.getLeadershipToken(versionedAquarium);
        Preconditions.checkState(!requiresOnline || livelyEndState.isOnline(), "Partition:%s state:%s is not online.",
            versionedPartitionName,
            livelyEndState);
        if (specificVersion.isPresent() && versionedPartitionName.getPartitionVersion() != specificVersion.get()) {
            return null;
        }
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            RowsChanged changes = storage.update(partitionIndex,
                directApply,
                partitionStore.getProperties().rowType,
                highwaterStorage,
                versionedPartitionName,
                stripe,
                partitionStore.getWalStorage(),
                prefix,
                updates,
                updated);
            if (allRowChanges != null && !changes.isEmpty()) {
                allRowChanges.changes(changes);
            }
            postCommit.committed(versionedPartitionName, leadershipToken, changes.getLargestCommittedTxId());
            return changes;
        }

    }

    public void flush(boolean fsync) throws Exception {
        storage.flush(fsync);
    }

    public boolean get(VersionedAquarium versionedAquarium, byte[] prefix, byte[] key, KeyValueStream keyValueStream) throws
        Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName, livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return storage.get(versionedPartitionName, partitionStore.getWalStorage(), prefix, (stream) -> stream.stream(key), keyValueStream);
        }
    }

    public boolean get(VersionedAquarium versionedAquarium, byte[] prefix, UnprefixedWALKeys keys, KeyValueStream stream) throws
        Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName, livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return storage.get(versionedPartitionName, partitionStore.getWalStorage(), prefix, keys, stream);
        }

    }

    public void rowScan(VersionedAquarium versionedAquarium, KeyValueStream keyValueStream) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName, livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            storage.rowScan(versionedPartitionName, partitionStore, keyValueStream);
        }

    }

    public void rangeScan(VersionedAquarium versionedAquarium,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueStream keyValueStream) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName,
            livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            storage.rangeScan(versionedPartitionName, partitionStore, fromPrefix, fromKey, toPrefix, toKey, keyValueStream);
        }

    }

    public interface TakeRowUpdates<R> {

        R give(VersionedPartitionName versionedPartitionName, LivelyEndState livelyEndState, RowStreamer streamer) throws Exception;
    }

    public interface RowStreamer {

        void stream(RowStream rowStream) throws Exception;
    }

    public void takeAllRows(VersionedAquarium versionedAquarium, RowStream rowStream) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        if (versionedPartitionName != null && livelyEndState != null) {
            PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
            if (partitionStore != null) {
                storage.takeAllRows(versionedPartitionName, partitionStore.getWalStorage(), rowStream);
            }
        }
    }

    public <R> R takeRowUpdatesSince(VersionedAquarium versionedAquarium,
        long transactionId,
        TakeRowUpdates<R> takeRowUpdates) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        if (versionedPartitionName == null || livelyEndState == null || livelyEndState.getCurrentState() == null) {
            return takeRowUpdates.give(null, null, null);
        }
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            return takeRowUpdates.give(null, null, null);
        } else {
            RowStreamer streamer = (livelyEndState.getCurrentState() != State.expunged)
                ? rowStream -> storage.takeRowsFromTransactionId(versionedPartitionName, partitionStore.getWalStorage(), transactionId, rowStream)
                : null;
            return takeRowUpdates.give(versionedPartitionName, livelyEndState, streamer);
        }
    }

    public WALHighwater takeFromTransactionId(VersionedAquarium versionedAquarium,
        long transactionId,
        HighwaterStorage highwaterStorage,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();

        WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName, livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            WALHighwater[] highwater = new WALHighwater[1];
            primaryRowMarshaller.fromRows(txFpRowStream -> {
                RowStream stream = (rowFP, rowTxId, rowType, row) -> {
                    if (rowType.isPrimary()) {
                        return txFpRowStream.stream(rowTxId, rowFP, rowType, row);
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

    }

    public WALHighwater takeFromTransactionId(VersionedAquarium versionedAquarium,
        byte[] prefix,
        long transactionId,
        HighwaterStorage highwaterStorage,
        Highwaters highwaters,
        TxKeyValueStream txKeyValueStream) throws Exception {

        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();

        WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName, livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            WALHighwater[] highwater = new WALHighwater[1];
            primaryRowMarshaller.fromRows(txFpRowStream -> {
                RowStream stream = (rowFP, rowTxId, rowType, row) -> {
                    if (rowType.isPrimary()) {
                        return txFpRowStream.stream(rowTxId, rowFP, rowType, row);
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

    }

    public long count(VersionedAquarium versionedAquarium) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();

        // any state is OK!
        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return storage.count(versionedPartitionName, partitionStore.getWalStorage());
        }

    }

    public boolean containsKeys(VersionedAquarium versionedAquarium, byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
        LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
        Preconditions.checkState(livelyEndState.isOnline(), "Partition:%s state:%s is not online.", versionedPartitionName, livelyEndState);

        PartitionStore partitionStore = partitionIndex.get(versionedPartitionName, stripe);
        if (partitionStore == null) {
            throw new IllegalStateException("No partition defined for " + versionedPartitionName);
        } else {
            return storage.containsKeys(versionedPartitionName, partitionStore.getWalStorage(), prefix, keys, stream);
        }

    }

    public boolean mergeable() {
        return storage.mergeable();
    }

    public void merge(boolean force) {
        try {
            storage.merge(partitionIndex, stripe, storageVersionProvider, force);
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
        return Objects.equals(this.name, other.name);
    }

}
