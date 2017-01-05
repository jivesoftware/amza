package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.RingMembership;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.api.wal.WALValue;
import com.jivesoftware.os.amza.service.AwaitNotify;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.PartitionIsDisposedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.partition.VersionedPartitionProvider;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.storage.delta.DeltaStripeWALStorage;
import com.jivesoftware.os.jive.utils.collections.lh.LHMapState;
import com.jivesoftware.os.jive.utils.collections.lh.LHash;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class StorageVersionProvider implements CurrentVersionProvider, RowChanges, SystemStriper {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final Random rand = new Random();

    private final BAInterner interner;
    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final SystemWALStorage systemWALStorage;
    private final VersionedPartitionProvider versionedPartitionProvider;
    private final RingMembership ringMembership;
    private final File[] workingIndexDirectories;
    private final FileLock[] stripeLocks;
    private final long[] stripeVersions;
    private final long stripeMaxFreeWithinNBytes;
    private final DeltaStripeWALStorage[] deltaStripeWALStorages;
    private final WALUpdated walUpdated;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final Map<PartitionName, StickyStorage> partitionStorage = Maps.newConcurrentMap();
    private final Map<RingMemberAndPartitionName, StorageVersion> remoteVersionCache = Maps.newConcurrentMap();

    public StorageVersionProvider(BAInterner interner,
        OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        SystemWALStorage systemWALStorage,
        VersionedPartitionProvider versionedPartitionProvider,
        RingMembership ringMembership,
        File[] workingIndexDirectories,
        long[] stripeVersions,
        FileLock[] stripeLocks,
        long stripeMaxFreeWithinNBytes,
        DeltaStripeWALStorage[] deltaStripeWALStorages,
        WALUpdated walUpdated,
        AwaitNotify<PartitionName> awaitNotify) {
        this.interner = interner;
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemWALStorage = systemWALStorage;
        this.versionedPartitionProvider = versionedPartitionProvider;
        this.ringMembership = ringMembership;
        this.workingIndexDirectories = workingIndexDirectories;
        this.stripeVersions = stripeVersions;
        this.stripeLocks = stripeLocks;
        this.stripeMaxFreeWithinNBytes = stripeMaxFreeWithinNBytes;
        this.deltaStripeWALStorages = deltaStripeWALStorages;
        this.walUpdated = walUpdated;
        this.awaitNotify = awaitNotify;
    }

    public void start() {
        for (int i = 0; i < stripeLocks.length; i++) {
            Preconditions.checkState(stripeLocks[i].isValid() && !stripeLocks[i].isShared());
        }
    }

    public void stop() {
        for (int i = 0; i < stripeLocks.length; i++) {
            try {
                stripeLocks[i].release();
            } catch (IOException x) {
                LOG.error("Failed to release stripe lock {} for {}", new Object[]{i, workingIndexDirectories[i]}, x);
            }
        }
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

    private StorageVersionProvider.StickyStorage getStickyStorage(PartitionName partitionName) {
        return partitionStorage.computeIfAbsent(partitionName, key -> {
            try {
                return new StorageVersionProvider.StickyStorage(getRawStorageVersion(partitionName));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private StorageVersion getRawStorageVersion(PartitionName partitionName) throws Exception {
        TimestampedValue rawState = systemWALStorage.getTimestampedValue(PartitionCreator.PARTITION_VERSION_INDEX, null,
            walKey(rootRingMember, partitionName));
        if (rawState != null) {
            return StorageVersion.fromBytes(rawState.getValue());
        } else {
            return null;
        }
    }

    public StorageVersion createIfAbsent(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new StorageVersion(0, 0);
        }

        StickyStorage stickyStorage = getStickyStorage(partitionName);
        int stripeIndex = getCurrentStripe(stickyStorage.storageVersion);
        if (stripeIndex == -1) {
            stickyStorage.semaphore.acquire(Short.MAX_VALUE);
            try {
                stickyStorage = getStickyStorage(partitionName);
                stripeIndex = (stickyStorage.storageVersion == null) ? -1 : getStripeIndex(stickyStorage.storageVersion.stripeVersion);
                if (stripeIndex == -1) {
                    if (!versionedPartitionProvider.hasPartition(partitionName)) {
                        throw new PropertiesNotPresentException("Properties missing for " + partitionName);
                    }
                    if (!ringMembership.isMemberOfRing(partitionName.getRingName(), 0)) {
                        throw new NotARingMemberException("Not a member of ring for " + partitionName);
                    }

                    long maxFree = 0;
                    long[] free = new long[workingIndexDirectories.length];
                    for (int i = 0; i < workingIndexDirectories.length; i++) {
                        free[i] = workingIndexDirectories[i].getFreeSpace();
                        if (free[i] > maxFree) {
                            maxFree = free[i];
                        }
                    }

                    List<Integer> eligible = Lists.newArrayList();
                    for (int i = 0; i < workingIndexDirectories.length; i++) {
                        long nearMaxFree = maxFree - free[i];
                        if (nearMaxFree <= stripeMaxFreeWithinNBytes) {
                            eligible.add(i);
                        }
                    }
                    if (eligible.isEmpty()) {
                        throw new IllegalStateException("No disk free");
                    }

                    Random r = new Random();
                    stripeIndex = eligible.get(r.nextInt(eligible.size()));
                    updateStickyStorage(partitionName, stickyStorage, orderIdProvider.nextId(), stripeIndex);
                }
            } finally {
                stickyStorage.semaphore.release(Short.MAX_VALUE);
            }
        }
        return stickyStorage.storageVersion;
    }

    @Override
    public <R> R tx(PartitionName partitionName, StorageVersion requireStorageVersion, StripeIndexs<R> tx) throws Exception {
        if (partitionName.isSystemPartition()) {
            return tx.tx(-1, getSystemStripe(partitionName), new StorageVersion(0, 0));
        }

        StorageVersionProvider.StickyStorage stickyStorage = getStickyStorage(partitionName);
        stickyStorage.semaphore.acquire();
        try {
            StorageVersion currentStorageVersion = stickyStorage.storageVersion;
            if (currentStorageVersion == null && requireStorageVersion == null) {
                return tx.tx(-1, -1, null);
            }

            if (currentStorageVersion == null) {
                throw new PartitionIsDisposedException("Partition " + partitionName + " is disposed");
            }
            if (requireStorageVersion != null) {
                Preconditions.checkArgument(currentStorageVersion.partitionVersion == requireStorageVersion.partitionVersion,
                    "Partition version has changed: %s != %s", currentStorageVersion.partitionVersion, requireStorageVersion.partitionVersion);
            }
            int stripeIndex = getCurrentStripe(currentStorageVersion);
            Preconditions.checkArgument(stripeIndex != -1,
                "Missing stripe index for %s with stripe version %s", partitionName, currentStorageVersion.stripeVersion);

            StickyStripe stickyStripe;
            synchronized (stickyStorage.stripeCache) {
                stickyStripe = stickyStorage.stripeCache.get(currentStorageVersion.partitionVersion);
                if (stickyStripe == null) {
                    VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, currentStorageVersion.partitionVersion);
                    int s = stripeIndex;
                    for (int i = 0; i < deltaStripeWALStorages.length; i++) {
                        DeltaStripeWALStorage deltaStripeWALStorage = deltaStripeWALStorages[i];
                        if (deltaStripeWALStorage.hasChangesFor(versionedPartitionName)) {
                            s = i;
                        }
                    }
                    stickyStripe = new StickyStripe(s);
                    stickyStorage.stripeCache.put(currentStorageVersion.partitionVersion, stickyStripe);
                }
                stickyStripe.acquired.incrementAndGet();
            }

            try {
                return tx.tx(stickyStripe.stripeIndex, stripeIndex, currentStorageVersion);
            } finally {
                stickyStripe.acquired.decrementAndGet();
            }
        } finally {
            stickyStorage.semaphore.release();
        }
    }

    @Override
    public void invalidateDeltaIndexCache(VersionedPartitionName versionedPartitionName) throws Exception {
        StickyStorage stickyStorage = getStickyStorage(versionedPartitionName.getPartitionName());
        stickyStorage.semaphore.acquire();
        try {
            synchronized (stickyStorage.stripeCache) {
                StickyStripe stickyStripe = stickyStorage.stripeCache.get(versionedPartitionName.getPartitionVersion());
                if (stickyStripe != null && stickyStripe.acquired.get() == 0) {
                    stickyStorage.stripeCache.remove(versionedPartitionName.getPartitionVersion());
                }
            }
        } finally {
            stickyStorage.semaphore.release();
        }
    }

    // Sucks but its our legacy
    @Override
    public int getSystemStripe(PartitionName partitionName) {
        return Math.abs(partitionName.hashCode() % stripeVersions.length);
    }

    private int getCurrentStripe(StorageVersion storageVersion) {
        return (storageVersion == null) ? -1 : getStripeIndex(storageVersion.stripeVersion);
    }

    @Override
    public boolean isCurrentVersion(VersionedPartitionName versionedPartitionName) {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        if (partitionName.isSystemPartition()) {
            return true;
        }
        StorageVersion storageVersion = getStickyStorage(partitionName).storageVersion;
        return storageVersion != null && storageVersion.partitionVersion == versionedPartitionName.getPartitionVersion();
    }

    @Override
    public void abandonVersion(VersionedPartitionName versionedPartitionName) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        StickyStorage stickyStorage = getStickyStorage(partitionName);
        stickyStorage.semaphore.acquire(Short.MAX_VALUE);
        try {
            StorageVersion storageVersion = getStickyStorage(partitionName).storageVersion;
            int stripe = (storageVersion == null) ? -1 : getStripeIndex(storageVersion.stripeVersion);
            if (stripe != -1 && storageVersion.partitionVersion <= versionedPartitionName.getPartitionVersion()) {
                updateStickyStorage(partitionName, stickyStorage, orderIdProvider.nextId(), stripe);
            }
        } finally {
            stickyStorage.semaphore.release(Short.MAX_VALUE);
        }
    }

    // call with all semaphores for partition
    void transitionStripe(VersionedPartitionName versionedPartitionName, StorageVersion requireStorageVersion, int rebalanceToStripe) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        StickyStorage stickyStorage = getStickyStorage(partitionName);
        StorageVersion currentStorageVersion = stickyStorage.storageVersion;
        if (requireStorageVersion.equals(currentStorageVersion)) {
            updateStickyStorage(partitionName, stickyStorage, requireStorageVersion.partitionVersion, rebalanceToStripe);
        } else {
            throw new IllegalStateException(
                "Failed to transition to versionedPartitionName:" + versionedPartitionName
                + " stripe:" + rebalanceToStripe
                + " from " + currentStorageVersion
                + " to " + requireStorageVersion);
        }
    }

    <V> V replaceOneWithAll(PartitionName partitionName, Callable<V> callable) throws Exception {
        StickyStorage stickyStorage = getStickyStorage(partitionName);

        stickyStorage.semaphore.release();
        try {
            stickyStorage.semaphore.acquire(Short.MAX_VALUE);
            try {
                return callable.call();
            } finally {
                stickyStorage.semaphore.release(Short.MAX_VALUE);
            }
        } finally {
            stickyStorage.semaphore.acquire();
        }
    }

    public interface PartitionMemberStorageVersionStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, StorageVersion storageVersion) throws Exception;
    }

    public void streamLocal(PartitionMemberStorageVersionStream stream) throws Exception {
        byte[] fromKey = walKey(rootRingMember, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);

        systemWALStorage.rangeScan(PartitionCreator.PARTITION_VERSION_INDEX, null, fromKey, null, toKey,
            (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {

                    int o = 0;
                    o++; //serializationVersion
                    int ringMemberLength = UIO.bytesInt(key, o);
                    o += 4;
                    RingMember ringMember = RingMember.fromBytes(key, o, ringMemberLength, interner);
                    o += ringMemberLength;
                    o += 4; // partitionNameLength
                    PartitionName partitionName = PartitionName.fromBytes(key, o, interner);
                    StorageVersion storageVersion = StorageVersion.fromBytes(value);

                    int stripe = getStripeIndex(storageVersion.stripeVersion);
                    if (stripe != -1) {
                        return stream.stream(partitionName, ringMember, storageVersion);
                    }
                }
                return true;
            }, true);
    }

    private int getStripeIndex(long stripeVersion) {
        for (int i = 0; i < stripeVersions.length; i++) {
            if (stripeVersions[i] == stripeVersion) {
                return i;
            }
        }
        return -1;
    }

    public static PartitionName fromKey(byte[] key, BAInterner interner) throws Exception {
        int o = 0;
        o++; //serializationVersion
        int ringMemberLength = UIO.bytesInt(key, o);
        o += 4;
        o += ringMemberLength;
        o += 4; // partitionNameLength
        return PartitionName.fromBytes(key, o, interner);
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

    private void updateStickyStorage(PartitionName partitionName, StickyStorage stickyStorage, long partitionVersion, int stripe) throws Exception {
        StorageVersion storageVersion = new StorageVersion(partitionVersion, stripeVersions[stripe]);
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, partitionVersion);

        StorageVersion cachedVersion = stickyStorage.storageVersion;
        if (cachedVersion != null && cachedVersion.equals(storageVersion)) {
            return;
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
        stickyStorage.storageVersion = storageVersion;
        //TODO anything to notify?
        //takeCoordinator.stateChanged(amzaRingReader, versionedPartitionName, commitableStorageVersion.state);
        //takeCoordinator.awakeCya();
    }

    public boolean remove(RingMember rootRingMember, VersionedPartitionName versionedPartitionName) throws Exception {
        StickyStorage stickyStorage = getStickyStorage(versionedPartitionName.getPartitionName());
        stickyStorage.semaphore.acquire(Short.MAX_VALUE);
        try {
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
            stickyStorage.storageVersion = null;
            stickyStorage.stripeCache.remove(versionedPartitionName.getPartitionVersion());
            return !rowsChanged.isEmpty();
        } finally {
            stickyStorage.semaphore.release(Short.MAX_VALUE);
        }
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.PARTITION_VERSION_INDEX.equals(changes.getVersionedPartitionName())) {
            for (Map.Entry<WALKey, WALValue> change : changes.getApply().entrySet()) {
                clearCache(change.getKey().key, change.getValue().getValue());
            }
        }
    }

    private void invalidateRemoteVersionCache(RingMember ringMember, PartitionName partitionName) {
        remoteVersionCache.remove(new RingMemberAndPartitionName(ringMember, partitionName));
    }

    void clearCache(byte[] walKey, byte[] walValue) throws Exception {
        int o = 0;
        o++; // serializationVersion
        int ringMemberLength = UIO.bytesInt(walKey, o);
        o += 4;
        RingMember ringMember = RingMember.fromBytes(walKey, o, ringMemberLength, interner);
        o += ringMemberLength;
        if (ringMember != null) {
            o += 4; // partitionNameLength
            PartitionName partitionName = PartitionName.fromBytes(walKey, o, interner);
            if (ringMember.equals(rootRingMember)) {
                if (walValue != null) {
                    StorageVersion storageVersion = StorageVersion.fromBytes(walValue);
                    LOG.warn("Received external row changes for partition {} version {}", partitionName, storageVersion);
                } else {
                    LOG.warn("Received external row changes for partition {} with no version", partitionName);
                }
            } else {
                invalidateRemoteVersionCache(ringMember, partitionName);
            }
        }
    }

    private static class StickyStorage {

        private final Semaphore semaphore = new Semaphore(Short.MAX_VALUE, true);
        private final LHash<StickyStripe> stripeCache = new LHash<>(new LHMapState<>(3, -1, -2));
        private volatile StorageVersion storageVersion;

        private StickyStorage(StorageVersion storageVersion) {
            this.storageVersion = storageVersion;
        }
    }

    private static class StickyStripe {

        private final int stripeIndex;
        private final AtomicLong acquired = new AtomicLong();

        private StickyStripe(int stripeIndex) {
            this.stripeIndex = stripeIndex;
        }
    }

    private static class RingMemberAndPartitionName {

        private final byte[] ringMemberBytes;

        private final boolean systemPartition;
        private final byte[] ringNameBytes;
        private final byte[] partitionNameBytes;

        private final int hash;

        public RingMemberAndPartitionName(RingMember ringMember, PartitionName partitionName) {
            this.ringMemberBytes = ringMember.leakBytes();

            this.systemPartition = partitionName.isSystemPartition();
            this.ringNameBytes = partitionName.getRingName();
            this.partitionNameBytes = partitionName.getName();

            this.hash = ringMember.hashCode() + 31 * partitionName.hashCode();
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

            if (systemPartition != that.systemPartition) {
                return false;
            }
            if (!Arrays.equals(ringMemberBytes, that.ringMemberBytes)) {
                return false;
            }
            if (!Arrays.equals(ringNameBytes, that.ringNameBytes)) {
                return false;
            }
            return Arrays.equals(partitionNameBytes, that.partitionNameBytes);

        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

}
