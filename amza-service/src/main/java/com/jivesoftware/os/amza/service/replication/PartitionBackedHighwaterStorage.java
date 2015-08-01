package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionBackedHighwaterStorage implements HighwaterStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
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
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        long flushHighwatersAfterNUpdates) {

        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.walUpdated = walUpdated;
        this.systemWALStorage = systemWALStorage;
        this.flushHighwatersAfterNUpdates = flushHighwatersAfterNUpdates;
    }

    @Override
    public boolean expunge(VersionedPartitionName versionedPartitionName) throws Exception {
        bigBird.acquire();
        try {
            for (ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> got : hostToPartitionToHighwaterUpdates.values()) {
                got.remove(versionedPartitionName);
            }
            byte[] fromKey = walKey(versionedPartitionName, null);
            byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
            long removeTimestamp = orderIdProvider.nextId();
            systemWALStorage.rangeScan(PartitionProvider.HIGHWATER_MARK_INDEX, null, fromKey, null, toKey,
                (prefix, key, value, valueTimestamp, valueTombstone) -> {
                    systemWALStorage.update(PartitionProvider.HIGHWATER_MARK_INDEX,
                        (highwaters, txKeyValueStream) -> txKeyValueStream.row(-1, prefix, key, value, removeTimestamp, true),
                        walUpdated);
                    return true;
                });
            return true;
        } finally {
            bigBird.release();
        }
    }

    byte[] walKey(VersionedPartitionName versionedPartitionName, RingMember member) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "version");
        UIO.writeByteArray(filer, versionedPartitionName.toBytes(), "partition");
        UIO.writeByteArray(filer, rootRingMember.toBytes(), "rootMember");
        if (member != null) {
            UIO.writeByteArray(filer, member.toBytes(), "member");
        }
        return filer.getBytes();
    }

    RingMember getMember(byte[] rawMember) throws Exception {
        HeapFiler filer = new HeapFiler(rawMember);
        UIO.readByte(filer, "version");
        UIO.readByteArray(filer, "partition");
        UIO.readByteArray(filer, "rootMember");
        return RingMember.fromBytes(UIO.readByteArray(filer, "member"));
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
                systemWALStorage.update(PartitionProvider.HIGHWATER_MARK_INDEX,
                    (highwater, scan) -> scan.row(-1, null, walKey(versionedPartitionName, member), null, orderIdProvider.nextId(), true),
                    walUpdated);
                partitionHighwaterUpdates.remove(versionedPartitionName);
            }
        } finally {
            bigBird.release();
        }
    }

    @Override
    public Long get(RingMember member, VersionedPartitionName versionedPartitionName) throws Exception {
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
            TimestampedValue got = systemWALStorage.get(PartitionProvider.HIGHWATER_MARK_INDEX, null, walKey(versionedPartitionName, member));
            long txtId = -1L;
            if (got != null) {
                txtId = UIO.bytesLong(got.getValue());
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
        systemWALStorage.rangeScan(PartitionProvider.HIGHWATER_MARK_INDEX, null, fromKey, null, toKey,
            (prefix, key, value, valueTimestamp, valueTombstone) -> {
                highwaters.add(new RingMemberHighwater(getMember(key), UIO.bytesLong(value)));
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
                systemWALStorage.update(PartitionProvider.HIGHWATER_MARK_INDEX,
                    (highwater, scan) -> {
                        long timestamp = orderIdProvider.nextId();
                        for (VersionedPartitionName versionedPartitionName : partitions.keySet()) {
                            if (!scan.row(-1, null, walKey(versionedPartitionName, member), null, timestamp, true)) {
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

                systemWALStorage.update(PartitionProvider.HIGHWATER_MARK_INDEX,
                    (highwater, scan) -> {
                        if (preFlush != null) {
                            preFlush.call();
                        }

                        long timestamp = orderIdProvider.nextId();
                        for (Entry<RingMember, ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates>> ringEntry
                            : hostToPartitionToHighwaterUpdates.entrySet()) {
                            RingMember ringMember = ringEntry.getKey();
                            for (Map.Entry<VersionedPartitionName, HighwaterUpdates> partitionEntry : ringEntry.getValue().entrySet()) {
                                HighwaterUpdates highwaterUpdates = partitionEntry.getValue();
                                if (highwaterUpdates != null && highwaterUpdates.updates.get() > 0) {
                                    try {
                                        long txId = highwaterUpdates.getTxId();
                                        int total = highwaterUpdates.updates.get();
                                        if (!scan.row(-1, null, walKey(partitionEntry.getKey(), ringMember), UIO.longBytes(txId), timestamp, false)) {
                                            return false;
                                        }
                                        highwaterUpdates.update(txId, -total);
                                    } catch (Exception x) {
                                        throw new RuntimeException();
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
        private final AtomicInteger updates = new AtomicInteger(0);

        public HighwaterUpdates() {
            this.txId = new AtomicLong(-1L);
        }

        public int update(long txId, int updates) {
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
