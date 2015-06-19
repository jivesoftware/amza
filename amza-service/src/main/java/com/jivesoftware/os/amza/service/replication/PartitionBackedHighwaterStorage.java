package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionBackedHighwaterStorage implements HighwaterStorage {

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final SystemWALStorage systemWALStorage;

    private final int numPermits = 1024;
    private final Semaphore bigBird = new Semaphore(numPermits, true); // TODO expose to config
    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates>> hostToPartitionToHighwaterUpdates =
        new ConcurrentHashMap<>();
    private final AtomicLong updatesSinceLastFlush = new AtomicLong();

    public PartitionBackedHighwaterStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        SystemWALStorage systemWALStorage) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemWALStorage = systemWALStorage;
    }

    @Override
    public boolean expunge(VersionedPartitionName versionedPartitionName) throws Exception {
        bigBird.acquire();
        try {
            for (ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> got : hostToPartitionToHighwaterUpdates.values()) {
                got.remove(versionedPartitionName);
            }
            WALKey from = walKey(versionedPartitionName, null);
            WALKey to = from.prefixUpperExclusive();
            long removeTimestamp = orderIdProvider.nextId();
            systemWALStorage.rangeScan(PartitionProvider.HIGHWATER_MARK_INDEX, from, to, (long rowTxId, WALKey key, WALValue value) -> {
                systemWALStorage.update(PartitionProvider.HIGHWATER_MARK_INDEX,
                    (highwaters, scan) -> {
                        scan.row(-1, key, new WALValue(value.getValue(), removeTimestamp, true));
                    });
                return true;
            });
            return true;
        } finally {
            bigBird.release();
        }
    }

    WALKey walKey(VersionedPartitionName versionedPartitionName, RingMember member) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "version");
        UIO.writeByteArray(filer, versionedPartitionName.toBytes(), "partition");
        UIO.writeByteArray(filer, rootRingMember.toBytes(), "rootMember");
        if (member != null) {
            UIO.writeByteArray(filer, member.toBytes(), "member");
        }
        return new WALKey(filer.getBytes());

    }

    @Override
    public void setIfLarger(final RingMember member, final VersionedPartitionName versionedPartitionName, int updates, long highwaterTxId) throws Exception {
        bigBird.acquire();
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
                    (highwater, scan) -> {
                        scan.row(-1, walKey(versionedPartitionName, member), new WALValue(null, orderIdProvider.nextId(), true));
                    });
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
            WALValue got = systemWALStorage.get(PartitionProvider.HIGHWATER_MARK_INDEX, walKey(versionedPartitionName, member));
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
        WALKey from = walKey(versionedPartitionName, null);
        WALKey to = from.prefixUpperExclusive();
        List<RingMemberHighwater> highwaters = new ArrayList<>();
        systemWALStorage.rangeScan(PartitionProvider.HIGHWATER_MARK_INDEX, from, to, (long rowTxId, WALKey key, WALValue value) -> {
            highwaters.add(new RingMemberHighwater(RingMember.fromBytes(key.getKey()), UIO.bytesLong(value.getValue())));
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
                            scan.row(-1, walKey(versionedPartitionName, member), new WALValue(null, timestamp, true));
                        }
                    });

            }
            hostToPartitionToHighwaterUpdates.remove(member);
        } finally {
            bigBird.release();
        }
    }

    @Override
    public void flush(Callable<Void> preFlush) throws Exception {
        if (updatesSinceLastFlush.get() < 10_000) { //  TODO config;
            return;
        }

        bigBird.acquire(numPermits);
        try {
            systemWALStorage.update(PartitionProvider.HIGHWATER_MARK_INDEX,
                (highwater, scan) -> {
                    if (preFlush != null) {
                        preFlush.call();
                    }

                    long timestamp = orderIdProvider.nextId();
                    hostToPartitionToHighwaterUpdates.forEach((RingMember ringMember, ConcurrentHashMap<VersionedPartitionName, HighwaterUpdates> u) -> {
                        u.forEach((VersionedPartitionName versionedPartitionName, HighwaterUpdates highwaterUpdates) -> {
                            if (highwaterUpdates != null && highwaterUpdates.updates.get() > 0) {
                                try {
                                    long txId = highwaterUpdates.getTxId();
                                    int total = highwaterUpdates.updates.get();
                                    scan.row(-1, walKey(versionedPartitionName, ringMember), new WALValue(UIO.longBytes(txId), timestamp, false));
                                    highwaterUpdates.update(txId, -total);
                                } catch (Exception x) {
                                    throw new RuntimeException();
                                }
                            }
                        });
                    });

                });
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
