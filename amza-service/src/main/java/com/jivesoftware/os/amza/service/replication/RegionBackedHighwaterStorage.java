package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALHighwater;
import com.jivesoftware.os.amza.shared.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RegionBackedHighwaterStorage implements HighwaterStorage {

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final RegionStripe systemRegionStripe;
    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<VersionedRegionName, HighwaterUpdates>> hostToRegionToHighwaterUpdates =
        new ConcurrentHashMap<>();

    public RegionBackedHighwaterStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        RegionStripe systemRegionStripe) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemRegionStripe = systemRegionStripe;
    }

    @Override
    public boolean expunge(VersionedRegionName versionedRegionName) throws Exception {
        for (ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> got : hostToRegionToHighwaterUpdates.values()) {
            got.remove(versionedRegionName);
        }
        WALKey from = walKey(versionedRegionName, null);
        WALKey to = from.prefixUpperExclusive();
        long removeTimestamp = orderIdProvider.nextId();
        systemRegionStripe.rangeScan(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(), from, to, (long rowTxId, WALKey key, WALValue value) -> {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(),
                Optional.absent(),
                false,
                (highwaters, scan) -> {
                    scan.row(-1, key, new WALValue(value.getValue(), removeTimestamp, true));
                });
            return true;
        });
        return true;
    }

    WALKey walKey(VersionedRegionName versionedRegionName, RingMember member) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "version");
        UIO.writeByteArray(filer, versionedRegionName.toBytes(), "region");
        UIO.writeByteArray(filer, rootRingMember.toBytes(), "rootMember");
        if (member != null) {
            UIO.writeByteArray(filer, member.toBytes(), "member");
        }
        return new WALKey(filer.getBytes());
    }

    @Override
    public void setIfLarger(final RingMember member, final VersionedRegionName versionedRegionName, int updates, long highwaterTxId) throws Exception {
        ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(member);
        if (regionHighwaterUpdates == null) {
            regionHighwaterUpdates = new ConcurrentHashMap<>();
            ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> had = hostToRegionToHighwaterUpdates.putIfAbsent(member, regionHighwaterUpdates);
            if (had != null) {
                regionHighwaterUpdates = had;
            }
        }
        HighwaterUpdates highwaterUpdates = new HighwaterUpdates();
        HighwaterUpdates had = regionHighwaterUpdates.putIfAbsent(versionedRegionName, highwaterUpdates);
        if (had != null) {
            highwaterUpdates = had;
        }
        highwaterUpdates.update(highwaterTxId, updates);
    }

    @Override
    public void clear(final RingMember member, final VersionedRegionName versionedRegionName) throws Exception {
        ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(member);
        if (regionHighwaterUpdates != null) {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(),
                Optional.absent(),
                false,
                (highwater, scan) -> {
                    scan.row(-1, walKey(versionedRegionName, member), new WALValue(null, orderIdProvider.nextId(), true));
                });
            regionHighwaterUpdates.remove(versionedRegionName);
        }
    }

    @Override
    public Long get(RingMember member, VersionedRegionName versionedRegionName) throws Exception {
        ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(member);
        if (regionHighwaterUpdates == null) {
            regionHighwaterUpdates = new ConcurrentHashMap<>();
            ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> had = hostToRegionToHighwaterUpdates.putIfAbsent(member, regionHighwaterUpdates);
            if (had != null) {
                regionHighwaterUpdates = had;
            }
        }
        HighwaterUpdates highwaterUpdates = regionHighwaterUpdates.get(versionedRegionName);
        if (highwaterUpdates == null) {
            WALValue got = systemRegionStripe.get(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(), walKey(versionedRegionName, member));
            long txtId = -1L;
            if (got != null) {
                txtId = UIO.bytesLong(got.getValue());
            }
            highwaterUpdates = new HighwaterUpdates();
            HighwaterUpdates hadHighwaterUpdates = regionHighwaterUpdates.putIfAbsent(versionedRegionName, highwaterUpdates);
            if (hadHighwaterUpdates != null) {
                highwaterUpdates = hadHighwaterUpdates;
            }
            highwaterUpdates.update(txtId, 0);
        }
        return highwaterUpdates.getTxId();
    }

    @Override
    public WALHighwater getRegionHighwater(VersionedRegionName versionedRegionName) throws Exception {
        WALKey from = walKey(versionedRegionName, null);
        WALKey to = from.prefixUpperExclusive();
        List<RingMemberHighwater> highwaters = new ArrayList<>();
        systemRegionStripe.rangeScan(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(), from, to, (long rowTxId, WALKey key, WALValue value) -> {
            highwaters.add(new RingMemberHighwater(RingMember.fromBytes(key.getKey()), UIO.bytesLong(value.getValue())));
            return true;
        });
        return new WALHighwater(highwaters);
    }

    @Override
    public void clearRing(final RingMember member) throws Exception {
        final ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> regions = hostToRegionToHighwaterUpdates.get(member);
        if (regions != null && !regions.isEmpty()) {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(),
                Optional.absent(),
                false,
                (highwater, scan) -> {
                    long timestamp = orderIdProvider.nextId();
                    for (VersionedRegionName versionedRegionName : regions.keySet()) {
                        scan.row(-1, walKey(versionedRegionName, member), new WALValue(null, timestamp, true));
                    }
                });

        }
        hostToRegionToHighwaterUpdates.remove(member);
    }

    @Override
    public void flush(final ListMultimap<RingMember, VersionedRegionName> memberToRegionNames) throws Exception {

        systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX.getRegionName(),
            Optional.absent(),
            false,
            (highwater, scan) -> {
                long timestamp = orderIdProvider.nextId();
                for (Entry<RingMember, Collection<VersionedRegionName>> e : memberToRegionNames.asMap().entrySet()) {
                    final ConcurrentHashMap<VersionedRegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(e.getKey());
                    if (regionHighwaterUpdates != null) {
                        for (VersionedRegionName versionedRegionName : e.getValue()) {
                            HighwaterUpdates highwaterUpdates = regionHighwaterUpdates.get(versionedRegionName);
                            if (highwaterUpdates != null && highwaterUpdates.updates.get() > 0) {
                                long txId = highwaterUpdates.getTxId();
                                int total = highwaterUpdates.updates.get();
                                scan.row(-1, walKey(versionedRegionName, e.getKey()), new WALValue(UIO.longBytes(txId), timestamp, false));
                                highwaterUpdates.update(txId, -total);
                            }
                        }
                    }
                }
            });
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
