package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.PropertiesNotPresentException;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class StorageVersionProvider implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final SystemWALStorage systemWALStorage;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final PartitionStripeFunction partitionStripeFunction;
    private final long[] stripeVersions;
    private final WALUpdated walUpdated;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final ConcurrentHashMap<PartitionName, StorageVersion> localVersionCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<RingMemberAndPartitionName, StorageVersion> remoteVersionCache = new ConcurrentHashMap<>();

    public StorageVersionProvider(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        SystemWALStorage systemWALStorage,
        VersionedPartitionProvider versionedPartitionProvider,
        PartitionStripeFunction partitionStripeFunction,
        long[] stripeVersions,
        WALUpdated walUpdated,
        AwaitNotify<PartitionName> awaitNotify) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemWALStorage = systemWALStorage;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.partitionStripeFunction = partitionStripeFunction;
        this.stripeVersions = stripeVersions;
        this.walUpdated = walUpdated;
        this.awaitNotify = awaitNotify;
    }

    private static byte[] walKey(RingMember member, PartitionName partitionName) throws Exception {
        byte[] memberBytes = member.toBytes();
        if (partitionName != null) {
            byte[] partitionNameBytes = partitionName.toBytes();
            byte[] asBytes = new byte[1 + 4 + memberBytes.length + 4 + partitionNameBytes.length];
            asBytes[0] = 0; // version
            UIO.intBytes(memberBytes.length, asBytes, 1);
            System.arraycopy(memberBytes, 0, asBytes, 1 + 4, memberBytes.length);
            UIO.intBytes(partitionNameBytes.length, asBytes, 1 + 4 + memberBytes.length);
            System.arraycopy(partitionNameBytes, 0, asBytes, 1 + 4 + memberBytes.length + 4, partitionNameBytes.length);
            return asBytes;
        } else {
            byte[] asBytes = new byte[1 + 4 + memberBytes.length];
            asBytes[0] = 0; // version
            UIO.intBytes(memberBytes.length, asBytes, 1);
            System.arraycopy(memberBytes, 0, asBytes, 1 + 4, memberBytes.length);
            return asBytes;
        }
    }

    private final StripingLocksProvider<PartitionName> versionStripingLocks = new StripingLocksProvider<>(1024);

    public StorageVersion createIfAbsent(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new StorageVersion(0, 0);
        }
        if (versionedPartitionProvider.getProperties(partitionName) == null) {
            throw new PropertiesNotPresentException("Properties missing for " + partitionName);
        }
        synchronized (versionStripingLocks.lock(partitionName, 0)) {
            StorageVersion storageVersion = localVersionCache.computeIfAbsent(partitionName, key -> {
                try {
                    TimestampedValue rawState = systemWALStorage.getTimestampedValue(PartitionCreator.PARTITION_VERSION_INDEX, null,
                        walKey(rootRingMember, partitionName));
                    if (rawState != null) {
                        return StorageVersion.fromBytes(rawState.getValue());
                    } else {
                        return null;
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to deserialize version", e);
                }
            });
            if (storageVersion == null || storageVersion.stripeVersion != stripeVersions[partitionStripeFunction.stripe(partitionName)]) {
                storageVersion = set(partitionName, orderIdProvider.nextId());
            }
            return storageVersion;
        }
    }

    public long getPartitionVersion(PartitionName partitionName) {
        if (partitionName.isSystemPartition()) {
            return 0;
        }
        StorageVersion storageVersion = localVersionCache.get(partitionName);
        return storageVersion != null ? storageVersion.partitionVersion : -1;
    }

    public interface PartitionMemberStorageVersionStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, StorageVersion storageVersion) throws Exception;
    }

    public void streamLocal(PartitionMemberStorageVersionStream stream) throws Exception {
        byte[] fromKey = walKey(rootRingMember, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        byte[] intBuffer = new byte[4];

        systemWALStorage.rangeScan(PartitionCreator.PARTITION_VERSION_INDEX, null, fromKey, null, toKey,
            (rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                HeapFiler filer = new HeapFiler(key);
                UIO.readByte(filer, "serializationVersion");
                RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member", intBuffer));
                PartitionName partitionName = PartitionName.fromBytes(UIO.readByteArray(filer, "partition", intBuffer));
                StorageVersion storageVersion = StorageVersion.fromBytes(value);

                if (storageVersion.stripeVersion == stripeVersions[partitionStripeFunction.stripe(partitionName)]) {
                    return stream.stream(partitionName, ringMember, storageVersion);
                } else {
                    return true;
                }
            });
    }

    public StorageVersion getRemote(RingMember ringMember, PartitionName partitionName) throws Exception {
        return remoteVersionCache.computeIfAbsent(new RingMemberAndPartitionName(ringMember, partitionName), key -> {
            try {
                TimestampedValue rawState = systemWALStorage.getTimestampedValue(PartitionCreator.PARTITION_VERSION_INDEX, null,
                    walKey(ringMember, partitionName));
                if (rawState == null) {
                    return null;
                }
                return StorageVersion.fromBytes(rawState.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private StorageVersion set(PartitionName partitionName, long partitionVersion) throws Exception {
        StorageVersion storageVersion = new StorageVersion(partitionVersion, stripeVersions[partitionStripeFunction.stripe(partitionName)]);
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, partitionVersion);
        StorageVersion cachedVersion = localVersionCache.get(partitionName);
        if (cachedVersion != null && cachedVersion.equals(storageVersion)) {
            return storageVersion;
        }

        byte[] versionedStateBytes = storageVersion.toBytes();
        awaitNotify.notifyChange(partitionName, () -> {
            long timestampAndVersion = orderIdProvider.nextId();
            RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.PARTITION_VERSION_INDEX, null,
                (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                    walKey(rootRingMember, partitionName),
                    versionedStateBytes, timestampAndVersion, false, timestampAndVersion),
                walUpdated);
            return !rowsChanged.isEmpty();
        });

        LOG.info("Storage version: {} {} was updated to {}", rootRingMember, versionedPartitionName, partitionVersion);
        localVersionCache.put(partitionName, storageVersion);
        //TODO anything to notify?
        //takeCoordinator.stateChanged(amzaRingReader, versionedPartitionName, commitableStorageVersion.state);
        //takeCoordinator.awakeCya();

        return storageVersion;
    }

    public boolean remove(RingMember rootRingMember, VersionedPartitionName versionedPartitionName) throws Exception {
        long timestampAndVersion = orderIdProvider.nextId();
        RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.PARTITION_VERSION_INDEX, null,
            (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                walKey(rootRingMember, versionedPartitionName.getPartitionName()),
                null,
                timestampAndVersion,
                true,
                timestampAndVersion),
            walUpdated);

        LOG.info("Storage version: {} {} was removed: {}", rootRingMember, versionedPartitionName, rowsChanged);
        invalidateLocalVersionCache(versionedPartitionName);
        return !rowsChanged.isEmpty();
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.PARTITION_VERSION_INDEX.equals(changes.getVersionedPartitionName())) {
            for (Map.Entry<WALKey, WALValue> change : changes.getApply().entrySet()) {
                clearCache(change.getKey().key, change.getValue().getValue());
            }
        }
    }

    private void invalidateLocalVersionCache(VersionedPartitionName versionedPartitionName) {
        localVersionCache.computeIfPresent(versionedPartitionName.getPartitionName(), (partitionName, versionedState) -> {
            if (versionedState.partitionVersion == versionedPartitionName.getPartitionVersion()) {
                return null;
            } else {
                return versionedState;
            }
        });
    }

    private void invalidateRemoteVersionCache(RingMember ringMember, PartitionName partitionName) {
        remoteVersionCache.remove(new RingMemberAndPartitionName(ringMember, partitionName));
    }

    void clearCache(byte[] walKey, byte[] walValue) throws Exception {
        byte[] intBuffer = new byte[4];

        HeapFiler filer = new HeapFiler(walKey);
        UIO.readByte(filer, "serializationVersion");
        RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member", intBuffer));
        if (ringMember != null) {
            PartitionName partitionName = PartitionName.fromBytes(UIO.readByteArray(filer, "partition", intBuffer));
            if (ringMember.equals(rootRingMember)) {
                StorageVersion storageVersion = StorageVersion.fromBytes(walValue);
                invalidateLocalVersionCache(new VersionedPartitionName(partitionName, storageVersion.partitionVersion));
            } else {
                invalidateRemoteVersionCache(ringMember, partitionName);
            }
        }
    }

    private static class RingMemberAndPartitionName {

        private final RingMember ringMember;
        private final PartitionName partitionName;

        public RingMemberAndPartitionName(RingMember ringMember, PartitionName partitionName) {
            this.ringMember = ringMember;
            this.partitionName = partitionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RingMemberAndPartitionName that = (RingMemberAndPartitionName) o;

            if (ringMember != null ? !ringMember.equals(that.ringMember) : that.ringMember != null) {
                return false;
            }
            return !(partitionName != null ? !partitionName.equals(that.partitionName) : that.partitionName != null);

        }

        @Override
        public int hashCode() {
            int result = ringMember != null ? ringMember.hashCode() : 0;
            result = 31 * result + (partitionName != null ? partitionName.hashCode() : 0);
            return result;
        }
    }

}
