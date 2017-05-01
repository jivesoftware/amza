package com.jivesoftware.os.amza.service.replication;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionBackedHighwaterStorage implements HighwaterStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaSystemStats;
    private final AmzaStats amzaStats;
    private final AmzaInterner memberInterner;
    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final PartitionCreator partitionCreator;
    private final SystemWALStorage systemWALStorage;
    private final WALUpdated walUpdated;
    private final long flushHighwatersAfterNUpdates;

    private final int numPermits = 1024;
    private final Semaphore bigBird = new Semaphore(numPermits, true); // TODO expose to config
    private final Map<RingMember, Map<VersionedPartitionName, HighwaterUpdates>> hostToPartitionToHighwaterUpdates = Maps.newConcurrentMap();
    private final Map<VersionedPartitionName, LocalHighwater> localHighwaterUpdates = Maps.newConcurrentMap();
    private final AtomicLong[] stripeUpdatesSinceLastFlush;
    private final AtomicLong systemUpdatesSinceLastFlush = new AtomicLong();

    public PartitionBackedHighwaterStorage(AmzaStats amzaSystemStats,
        AmzaStats amzaStats,
        AmzaInterner memberInterner,
        OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        PartitionCreator partitionCreator,
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        long flushHighwatersAfterNUpdates,
        int deltaStripeCount) {

        this.amzaSystemStats = amzaSystemStats;
        this.amzaStats = amzaStats;
        this.memberInterner = memberInterner;
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.partitionCreator = partitionCreator;
        this.systemWALStorage = systemWALStorage;
        this.walUpdated = walUpdated;
        this.flushHighwatersAfterNUpdates = flushHighwatersAfterNUpdates;

        this.stripeUpdatesSinceLastFlush = new AtomicLong[deltaStripeCount];
        for (int i = 0; i < deltaStripeCount; i++) {
            stripeUpdatesSinceLastFlush[i] = new AtomicLong();
        }
    }

    public static void main(String[] args) throws Exception {
        AmzaInterner amzaInterner = new AmzaInterner();
        PartitionName a = amzaInterner.internPartitionNameBase64("AAAAAAABYQAAAARhNzIw");
        System.out.println(a);
    }

    @Override
    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
        bigBird.acquire();
        try {
            for (Map<VersionedPartitionName, HighwaterUpdates> got : hostToPartitionToHighwaterUpdates.values()) {
                got.remove(versionedPartitionName);
            }
            byte[] fromKey = walKey(versionedPartitionName, null);
            byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
            long removeTimestamp = orderIdProvider.nextId();
            systemWALStorage.rangeScan(PartitionCreator.HIGHWATER_MARK_INDEX, null, fromKey, null, toKey,
                (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    // could skip entries with valueTombstoned, but we ensure better consistency by adding a tombstone with a newer timestamp
                    systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, prefix,
                        (highwaters, txKeyValueStream) -> txKeyValueStream.row(-1, key, value, removeTimestamp, true, removeTimestamp),
                        walUpdated);
                    return true;
                }, true);
        } finally {
            bigBird.release();
        }
    }

    byte[] walKey(VersionedPartitionName versionedPartitionName, RingMember member) throws IOException {
        byte[] versionedPartitionNameBytes = versionedPartitionName.toBytes();
        byte[] rootRingMemberBytes = rootRingMember.toBytes();
        if (member != null) {
            byte[] memberBytes = member.toBytes();
            byte[] asBytes = new byte[1 + 4 + versionedPartitionNameBytes.length + 4 + rootRingMemberBytes.length + 4 + memberBytes.length];
            asBytes[0] = 0; // version
            UIO.intBytes(versionedPartitionNameBytes.length, asBytes, 1);
            System.arraycopy(versionedPartitionNameBytes, 0, asBytes, 1 + 4, versionedPartitionNameBytes.length);
            UIO.intBytes(rootRingMemberBytes.length, asBytes, 1 + 4 + versionedPartitionNameBytes.length);
            System.arraycopy(rootRingMemberBytes, 0, asBytes, 1 + 4 + versionedPartitionNameBytes.length + 4, rootRingMemberBytes.length);
            UIO.intBytes(memberBytes.length, asBytes, 1 + 4 + versionedPartitionNameBytes.length + 4 + rootRingMemberBytes.length);
            System.arraycopy(memberBytes, 0, asBytes, 1 + 4 + versionedPartitionNameBytes.length + 4 + rootRingMemberBytes.length + 4, memberBytes.length);
            return asBytes;
        } else {
            byte[] asBytes = new byte[1 + 4 + versionedPartitionNameBytes.length + 4 + rootRingMemberBytes.length];
            asBytes[0] = 0; // version
            UIO.intBytes(versionedPartitionNameBytes.length, asBytes, 1);
            System.arraycopy(versionedPartitionNameBytes, 0, asBytes, 1 + 4, versionedPartitionNameBytes.length);
            UIO.intBytes(rootRingMemberBytes.length, asBytes, 1 + 4 + versionedPartitionNameBytes.length);
            System.arraycopy(rootRingMemberBytes, 0, asBytes, 1 + 4 + versionedPartitionNameBytes.length + 4, rootRingMemberBytes.length);
            return asBytes;
        }
    }

    RingMember getMember(byte[] rawMember) throws Exception {
        int o = 0;
        o += 1; //version
        o += UIO.bytesInt(rawMember, o); // partition
        o += 4;
        o += UIO.bytesInt(rawMember, o); // rootMember
        o += 4;
        int ringMemberLength = UIO.bytesInt(rawMember, o);
        o += 4;
        return memberInterner.internRingMember(rawMember, o, ringMemberLength);
    }

    @Override
    public void setIfLarger(RingMember member,
        VersionedPartitionName versionedPartitionName,
        long highwaterTxId,
        int deltaIndex,
        int updates) throws Exception {

        if (member.equals(rootRingMember)) {
            return;
        }

        bigBird.acquire();
        if (deltaIndex == -1) {
            long pending = systemUpdatesSinceLastFlush.addAndGet(updates);
            amzaSystemStats.highwater(0, -1, pending, pending / (double) flushHighwatersAfterNUpdates);
        } else {
            long pending = stripeUpdatesSinceLastFlush[deltaIndex].addAndGet(updates);
            amzaStats.highwater(deltaIndex, -1, pending, pending / (double) flushHighwatersAfterNUpdates);
        }
        try {
            Map<VersionedPartitionName, HighwaterUpdates> partitionHighwaterUpdates = hostToPartitionToHighwaterUpdates.computeIfAbsent(member,
                (t) -> Maps.newConcurrentMap());
            HighwaterUpdates highwaterUpdates = partitionHighwaterUpdates.computeIfAbsent(versionedPartitionName, (t) -> new HighwaterUpdates());
            highwaterUpdates.updateTxId(highwaterTxId);
            if (updates > 0) {
                highwaterUpdates.addDeltaUpdates(deltaIndex, updates);
            }
        } finally {
            bigBird.release();
        }
    }

    @Override
    public void clear(RingMember member, VersionedPartitionName versionedPartitionName) throws Exception {
        bigBird.acquire();
        try {
            Map<VersionedPartitionName, HighwaterUpdates> partitionHighwaterUpdates = hostToPartitionToHighwaterUpdates.get(member);
            if (partitionHighwaterUpdates != null) {
                long timestampAndVersion = orderIdProvider.nextId();
                systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                    (highwater, scan) -> scan.row(-1, walKey(versionedPartitionName, member), null, timestampAndVersion, true, timestampAndVersion),
                    walUpdated);
                partitionHighwaterUpdates.remove(versionedPartitionName);
            }
        } finally {
            bigBird.release();
        }
    }

    @Override
    public long get(RingMember member, VersionedPartitionName versionedPartitionName) throws Exception {
        Map<VersionedPartitionName, HighwaterUpdates> partitionHighwaterUpdates = hostToPartitionToHighwaterUpdates.computeIfAbsent(member,
            (t) -> Maps.newConcurrentMap());
        HighwaterUpdates highwaterUpdates = partitionHighwaterUpdates.get(versionedPartitionName);
        if (highwaterUpdates == null) {
            PartitionProperties partitionProperties = partitionCreator.getProperties(versionedPartitionName.getPartitionName());
            long txId = -1L;
            if (partitionProperties.durability != Durability.ephemeral) {
                TimestampedValue got = systemWALStorage.getTimestampedValue(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                    walKey(versionedPartitionName, member));
                if (got != null) {
                    txId = UIO.bytesLong(got.getValue());
                }
            }
            highwaterUpdates = partitionHighwaterUpdates.computeIfAbsent(versionedPartitionName, (t) -> new HighwaterUpdates());
            highwaterUpdates.updateTxId(txId);
        }
        return highwaterUpdates.getTxId();

    }

    @Override
    public WALHighwater getPartitionHighwater(VersionedPartitionName versionedPartitionName, boolean includeLocal) throws Exception {
        byte[] fromKey = walKey(versionedPartitionName, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        List<RingMemberHighwater> highwaters = new ArrayList<>();
        systemWALStorage.rangeScan(PartitionCreator.HIGHWATER_MARK_INDEX, null, fromKey, null, toKey,
            (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    RingMember member = getMember(key);
                    if (includeLocal || !member.equals(rootRingMember)) {
                        highwaters.add(new RingMemberHighwater(member, UIO.bytesLong(value)));
                    }
                }
                return true;
            }, true);
        return new WALHighwater(highwaters);
    }

    @Override
    public void clearRing(final RingMember member) throws Exception {
        bigBird.acquire();
        try {
            final Map<VersionedPartitionName, HighwaterUpdates> partitions = hostToPartitionToHighwaterUpdates.get(member);
            if (partitions != null && !partitions.isEmpty()) {
                systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                    (highwater, scan) -> {
                        long timestampAndVersion = orderIdProvider.nextId();
                        for (VersionedPartitionName versionedPartitionName : partitions.keySet()) {
                            if (!scan.row(-1, walKey(versionedPartitionName, member), null, timestampAndVersion, true, timestampAndVersion)) {
                                return false;
                            }
                        }
                        return true;
                    }, walUpdated);

            }
            hostToPartitionToHighwaterUpdates.remove(member);
        } finally {
            bigBird.release();
        }
    }

    @Override
    public boolean flush(int deltaIndex, boolean force, Callable<Void> preFlush) throws Exception {
        AtomicLong updatesSinceLastFlush;
        AmzaStats stats;
        if (deltaIndex == -1) {
            updatesSinceLastFlush = systemUpdatesSinceLastFlush;
            stats = amzaSystemStats;
        } else {
            updatesSinceLastFlush = stripeUpdatesSinceLastFlush[deltaIndex];
            stats = amzaStats;
        }
        if (!force && updatesSinceLastFlush.get() < flushHighwatersAfterNUpdates) {
            return false;
        }
        bigBird.acquire(numPermits);
        try {
            long flushedUpdates = updatesSinceLastFlush.get();
            if (!force && flushedUpdates < flushHighwatersAfterNUpdates) {
                return false;
            } else {
                systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                    (highwater, scan) -> {
                        if (preFlush != null) {
                            preFlush.call();
                        }

                        long timestampAndVersion = orderIdProvider.nextId();
                        for (Entry<RingMember, Map<VersionedPartitionName, HighwaterUpdates>> ringEntry : hostToPartitionToHighwaterUpdates.entrySet()) {
                            RingMember ringMember = ringEntry.getKey();
                            for (Map.Entry<VersionedPartitionName, HighwaterUpdates> partitionEntry : ringEntry.getValue().entrySet()) {
                                VersionedPartitionName versionedPartitionName = partitionEntry.getKey();
                                PartitionProperties properties = partitionCreator.getProperties(versionedPartitionName.getPartitionName());
                                if (properties.durability != Durability.ephemeral) {
                                    HighwaterUpdates highwaterUpdates = partitionEntry.getValue();
                                    if (highwaterUpdates != null) {
                                        AtomicLong updates = highwaterUpdates.updates.get(deltaIndex);
                                        if (updates != null && updates.get() > 0) {
                                            long txId = highwaterUpdates.getTxId();
                                            long total = updates.get();
                                            if (!scan.row(-1, walKey(versionedPartitionName, ringMember),
                                                UIO.longBytes(txId), timestampAndVersion, false, timestampAndVersion)) {
                                                return false;
                                            }
                                            highwaterUpdates.updateTxId(txId);
                                            highwaterUpdates.addDeltaUpdates(deltaIndex, -total);
                                        }
                                    }
                                }
                            }
                        }
                        return true;

                    }, walUpdated);

                long pending = updatesSinceLastFlush.addAndGet(-flushedUpdates);
                stats.highwater(deltaIndex == -1 ? 0 : deltaIndex, flushedUpdates, pending, pending / (double) flushHighwatersAfterNUpdates);
                return true;
            }
        } finally {
            bigBird.release(numPermits);
        }
    }

    private static class HighwaterUpdates {

        private final AtomicLong txId;
        private final Map<Integer, AtomicLong> updates = Maps.newConcurrentMap();

        public HighwaterUpdates() {
            this.txId = new AtomicLong(-1L);
        }

        public void updateTxId(long txId) {
            long got = this.txId.longValue();
            while (txId > got) {
                if (this.txId.compareAndSet(got, txId)) {
                    break;
                } else {
                    got = this.txId.get();
                }
            }
        }

        public long addDeltaUpdates(int deltaIndex, long updates) {
            return this.updates.computeIfAbsent(deltaIndex, k -> new AtomicLong()).addAndGet(updates);
        }

        public long getTxId() {
            return txId.get();
        }
    }

    @Override
    public void setLocal(VersionedPartitionName versionedPartitionName, long highwaterTxId) {
        LocalHighwater highwater = localHighwaterUpdates.computeIfAbsent(versionedPartitionName, versionedPartitionName1 -> new LocalHighwater());
        highwater.highwaterTxId.accumulateAndGet(highwaterTxId, Math::max);
    }

    @Override
    public long getLocal(VersionedPartitionName versionedPartitionName) throws Exception {
        LocalHighwater highwater = localHighwaterUpdates.computeIfAbsent(versionedPartitionName, versionedPartitionName1 -> new LocalHighwater());
        long txId = highwater.highwaterTxId.get();
        if (txId == LOCAL_NONE) {
            // can't call systemWALStorage inside of highwater lock due to flushLocal lock order
            TimestampedValue got = systemWALStorage.getTimestampedValue(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                walKey(versionedPartitionName, rootRingMember));
            synchronized (highwater) {
                long latestTxId = highwater.highwaterTxId.get();
                if (latestTxId == LOCAL_NONE) {
                    if (got != null) {
                        txId = UIO.bytesLong(got.getValue());
                    }
                    highwater.highwaterTxId.set(txId);
                    highwater.flushedTxId.set(txId);
                } else {
                    // somebody else won the race
                    txId = latestTxId;
                }
            }
        }
        return txId;
    }

    @Override
    public void flushLocal() throws Exception {
        systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, null,
            (highwaters, scan) -> {
                long timestampAndVersion = orderIdProvider.nextId();
                for (Entry<VersionedPartitionName, LocalHighwater> partitionEntry : localHighwaterUpdates.entrySet()) {
                    VersionedPartitionName versionedPartitionName = partitionEntry.getKey();
                    PartitionProperties properties = partitionCreator.getProperties(versionedPartitionName.getPartitionName());
                    if (properties.durability != Durability.ephemeral) {
                        LocalHighwater highwater = partitionEntry.getValue();
                        synchronized (highwater) {
                            long flushedTxId = highwater.flushedTxId.get();
                            long highwaterTxId = highwater.highwaterTxId.get();
                            if (flushedTxId < highwaterTxId) {
                                boolean result = scan.row(-1, walKey(versionedPartitionName, rootRingMember),
                                    UIO.longBytes(highwaterTxId), timestampAndVersion, false, timestampAndVersion);
                                highwater.flushedTxId.set(highwaterTxId);
                                if (!result) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                return true;

            }, walUpdated);
        systemWALStorage.flush(PartitionCreator.HIGHWATER_MARK_INDEX);
    }

    private static class LocalHighwater {
        private final AtomicLong highwaterTxId = new AtomicLong(LOCAL_NONE);
        private final AtomicLong flushedTxId = new AtomicLong(LOCAL_NONE);
    }
}
