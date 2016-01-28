package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionBackedHighwaterStorage implements HighwaterStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final PartitionIndex partitionIndex;
    private final SystemWALStorage systemWALStorage;
    private final WALUpdated walUpdated;
    private final long flushHighwatersAfterNUpdates;

    private final int numPermits = 1024;
    private final Semaphore bigBird = new Semaphore(numPermits, true); // TODO expose to config
    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates>> hostToPartitionToHighwaterUpdates =
        new ConcurrentHashMap<>();
    private final AtomicLong updatesSinceLastFlush = new AtomicLong();

    public PartitionBackedHighwaterStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        PartitionIndex partitionIndex,
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        long flushHighwatersAfterNUpdates) {

        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.partitionIndex = partitionIndex;
        this.systemWALStorage = systemWALStorage;
        this.walUpdated = walUpdated;
        this.flushHighwatersAfterNUpdates = flushHighwatersAfterNUpdates;
    }

    @Override
    public void delete(VersionedPartitionName versionedPartitionName) throws Exception {
        bigBird.acquire();
        try {
            for (ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> got : hostToPartitionToHighwaterUpdates.values()) {
                got.remove(versionedPartitionName);
            }
            byte[] fromKey = walKey(versionedPartitionName, null);
            byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
            long removeTimestamp = orderIdProvider.nextId();
            systemWALStorage.rangeScan(PartitionCreator.HIGHWATER_MARK_INDEX, null, fromKey, null, toKey,
                (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    // could skip entries with valueTombstoned, but we ensure better consistency by adding a tombstone with a newer timestamp
                    systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, prefix,
                        (highwaters, txKeyValueStream) -> txKeyValueStream.row(-1, key, value, removeTimestamp, true, removeTimestamp),
                        walUpdated);
                    return true;
                });
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
        byte[] intBuffer = new byte[4];

        HeapFiler filer = HeapFiler.fromBytes(rawMember, rawMember.length);
        UIO.readByte(filer, "version");
        UIO.readByteArray(filer, "partition", intBuffer);
        UIO.readByteArray(filer, "rootMember", intBuffer);
        return RingMember.fromBytes(UIO.readByteArray(filer, "member", intBuffer));
    }

    @Override
    public void setIfLarger(final RingMember member, final VersionedPartitionName versionedPartitionName, int updates, long highwaterTxId) throws Exception {
        if (member.equals(rootRingMember)) {
            return;
        }

        bigBird.acquire();
        updatesSinceLastFlush.addAndGet(updates);
        try {
            ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> partitionHighwaterUpdates = hostToPartitionToHighwaterUpdates.get(member);
            if (partitionHighwaterUpdates == null) {
                partitionHighwaterUpdates = new ConcurrentHashMap<>();
                ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> had = hostToPartitionToHighwaterUpdates.putIfAbsent(member,
                    partitionHighwaterUpdates);
                if (had != null) {
                    partitionHighwaterUpdates = had;
                }
            }
            HighwaterUpdates highwaterUpdates = new HighwaterUpdates();
            HighwaterUpdates had = partitionHighwaterUpdates.putIfAbsent(versionedPartitionName, highwaterUpdates);
            if (had != null) {
                highwaterUpdates = had;
            }
            highwaterUpdates.update(highwaterTxId, updates);
        } finally {
            bigBird.release();
        }
    }

    @Override
    public void clear(final RingMember member, final VersionedPartitionName versionedPartitionName) throws Exception {
        bigBird.acquire();
        try {
            ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> partitionHighwaterUpdates = hostToPartitionToHighwaterUpdates.get(member);
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
        ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> partitionHighwaterUpdates = hostToPartitionToHighwaterUpdates.get(member);
        if (partitionHighwaterUpdates == null) {
            partitionHighwaterUpdates = new ConcurrentHashMap<>();
            ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> had = hostToPartitionToHighwaterUpdates.putIfAbsent(member,
                partitionHighwaterUpdates);
            if (had != null) {
                partitionHighwaterUpdates = had;
            }
        }
        HighwaterUpdates highwaterUpdates = partitionHighwaterUpdates.get(versionedPartitionName);
        if (highwaterUpdates == null) {
            PartitionProperties partitionProperties = partitionIndex.getProperties(versionedPartitionName.getPartitionName());
            long txtId = -1L;
            if (partitionProperties.durability != Durability.ephemeral) {
                TimestampedValue got = systemWALStorage.getTimestampedValue(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                    walKey(versionedPartitionName, member));
                if (got != null) {
                    txtId = UIO.bytesLong(got.getValue());
                }
            }
            highwaterUpdates = new HighwaterUpdates();
            HighwaterUpdates hadHighwaterUpdates = partitionHighwaterUpdates.putIfAbsent(versionedPartitionName, highwaterUpdates);
            if (hadHighwaterUpdates != null) {
                highwaterUpdates = hadHighwaterUpdates;
            }
            highwaterUpdates.update(txtId, 0);
        }
        return highwaterUpdates.getTxId();

    }

    @Override
    public WALHighwater getPartitionHighwater(VersionedPartitionName versionedPartitionName) throws Exception {
        byte[] fromKey = walKey(versionedPartitionName, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        List<RingMemberHighwater> highwaters = new ArrayList<>();
        systemWALStorage.rangeScan(PartitionCreator.HIGHWATER_MARK_INDEX, null, fromKey, null, toKey,
            (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    highwaters.add(new RingMemberHighwater(getMember(key), UIO.bytesLong(value)));
                }
                return true;
            });
        return new WALHighwater(highwaters);
    }

    @Override
    public void clearRing(final RingMember member) throws Exception {
        bigBird.acquire();
        try {
            final ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> partitions = hostToPartitionToHighwaterUpdates.get(member);
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
    public void flush(Callable<Void> preFlush) throws Exception {
        if (updatesSinceLastFlush.get() < flushHighwatersAfterNUpdates) {
            return;
        }
        bigBird.acquire(numPermits);
        try {
            long flushedUpdates = updatesSinceLastFlush.get();
            if (flushedUpdates > flushHighwatersAfterNUpdates) {

                systemWALStorage.update(PartitionCreator.HIGHWATER_MARK_INDEX, null,
                    (highwater, scan) -> {
                        if (preFlush != null) {
                            preFlush.call();
                        }

                        long timestampAndVersion = orderIdProvider.nextId();
                        for (Entry<RingMember, ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates>> ringEntry
                            : hostToPartitionToHighwaterUpdates.entrySet()) {
                            RingMember ringMember = ringEntry.getKey();
                            for (Map.Entry<VersionedPartitionName, HighwaterUpdates> partitionEntry : ringEntry.getValue().entrySet()) {
                                VersionedPartitionName versionedPartitionName = partitionEntry.getKey();
                                PartitionProperties properties = partitionIndex.getProperties(versionedPartitionName.getPartitionName());
                                if (properties.durability != Durability.ephemeral) {
                                    HighwaterUpdates highwaterUpdates = partitionEntry.getValue();
                                    if (highwaterUpdates != null && highwaterUpdates.updates.get() > 0) {
                                        try {
                                            long txId = highwaterUpdates.getTxId();
                                            long total = highwaterUpdates.updates.get();
                                            if (!scan.row(-1, walKey(versionedPartitionName, ringMember),
                                                UIO.longBytes(txId), timestampAndVersion, false, timestampAndVersion)) {
                                                return false;
                                            }
                                            highwaterUpdates.update(txId, -total);
                                        } catch (Exception x) {
                                            throw new RuntimeException();
                                        }
                                    }
                                }
                            }
                        }
                        return true;

                    }, walUpdated);
                updatesSinceLastFlush.addAndGet(-flushedUpdates);
            }
        } finally {
            bigBird.release(numPermits);
        }
    }

    static class HighwaterUpdates {

        private final AtomicLong txId;
        private final AtomicLong updates = new AtomicLong(0);

        public HighwaterUpdates() {
            this.txId = new AtomicLong(-1L);
        }

        public long update(long txId, long updates) {
            long got = this.txId.longValue();
            while (txId > got) {
                if (this.txId.compareAndSet(got, txId)) {
                    break;
                } else {
                    got = this.txId.get();
                }
            }
            return this.updates.addAndGet(updates);
        }

        public long getTxId() {
            return txId.get();
        }
    }
}
