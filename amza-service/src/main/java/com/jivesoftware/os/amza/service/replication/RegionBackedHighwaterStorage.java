package com.jivesoftware.os.amza.service.replication;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.WALHighwater;
import com.jivesoftware.os.amza.shared.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
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
    private final WALReplicator replicator;
    private final ConcurrentHashMap<RingMember, ConcurrentHashMap<RegionName, HighwaterUpdates>> hostToRegionToHighwaterUpdates = new ConcurrentHashMap<>();

    public RegionBackedHighwaterStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        RegionStripe systemRegionStripe,
        WALReplicator replicator) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemRegionStripe = systemRegionStripe;
        this.replicator = replicator;
    }

    WALKey walKey(RegionName regionName, RingMember member) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "version");
        UIO.writeByteArray(filer, regionName.toBytes(), "region");
        UIO.writeByteArray(filer, rootRingMember.toBytes(), "rootMember");
        if (member != null) {
            UIO.writeByteArray(filer, member.toBytes(), "member");
        }
        return new WALKey(filer.getBytes());
    }

    @Override
    public void setIfLarger(final RingMember member, final RegionName regionName, int updates, long highwaterTxId) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(member);
        if (regionHighwaterUpdates == null) {
            regionHighwaterUpdates = new ConcurrentHashMap<>();
            ConcurrentHashMap<RegionName, HighwaterUpdates> had = hostToRegionToHighwaterUpdates.putIfAbsent(member, regionHighwaterUpdates);
            if (had != null) {
                regionHighwaterUpdates = had;
            }
        }
        HighwaterUpdates highwaterUpdates = new HighwaterUpdates();
        HighwaterUpdates had = regionHighwaterUpdates.putIfAbsent(regionName, highwaterUpdates);
        if (had != null) {
            highwaterUpdates = had;
        }
        highwaterUpdates.update(highwaterTxId, updates);
    }

    @Override
    public void clear(final RingMember member, final RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(member);
        if (regionHighwaterUpdates != null) {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (highwater, scan) -> {
                scan.row(-1, walKey(regionName, member), new WALValue(null, orderIdProvider.nextId(), true));
            });
            regionHighwaterUpdates.remove(regionName);
        }
    }

    @Override
    public Long get(RingMember member, RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(member);
        if (regionHighwaterUpdates == null) {
            regionHighwaterUpdates = new ConcurrentHashMap<>();
            ConcurrentHashMap<RegionName, HighwaterUpdates> had = hostToRegionToHighwaterUpdates.putIfAbsent(member, regionHighwaterUpdates);
            if (had != null) {
                regionHighwaterUpdates = had;
            }
        }
        HighwaterUpdates highwaterUpdates = regionHighwaterUpdates.get(regionName);
        if (highwaterUpdates == null) {
            WALValue got = systemRegionStripe.get(RegionProvider.HIGHWATER_MARK_INDEX, walKey(regionName, member));
            long txtId = -1L;
            if (got != null) {
                txtId = UIO.bytesLong(got.getValue());
            }
            highwaterUpdates = new HighwaterUpdates();
            HighwaterUpdates hadHighwaterUpdates = regionHighwaterUpdates.putIfAbsent(regionName, highwaterUpdates);
            if (hadHighwaterUpdates != null) {
                highwaterUpdates = hadHighwaterUpdates;
            }
            highwaterUpdates.update(txtId, 0);
        }
        return highwaterUpdates.getTxId();
    }

    @Override
    public void clearRing(final RingMember member) throws Exception {
        final ConcurrentHashMap<RegionName, HighwaterUpdates> regions = hostToRegionToHighwaterUpdates.get(member);
        if (regions != null && !regions.isEmpty()) {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (highwater, scan) -> {
                long timestamp = orderIdProvider.nextId();
                for (RegionName regionName : regions.keySet()) {
                    scan.row(-1, walKey(regionName, member), new WALValue(null, timestamp, true));
                }
            });

        }
        hostToRegionToHighwaterUpdates.remove(member);
    }

    @Override
    public void flush(final ListMultimap<RingMember, RegionName> memberToRegionNames) throws Exception {

        systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (highwater, scan) -> {
            long timestamp = orderIdProvider.nextId();
            for (Entry<RingMember, Collection<RegionName>> e : memberToRegionNames.asMap().entrySet()) {
                final ConcurrentHashMap<RegionName, HighwaterUpdates> regionHighwaterUpdates = hostToRegionToHighwaterUpdates.get(e.getKey());
                if (regionHighwaterUpdates != null) {
                    for (RegionName regionName : e.getValue()) {
                        HighwaterUpdates highwaterUpdates = regionHighwaterUpdates.get(regionName);
                        if (highwaterUpdates != null && highwaterUpdates.updates.get() > 0) {
                            long txId = highwaterUpdates.getTxId();
                            int total = highwaterUpdates.updates.get();
                            scan.row(-1, walKey(regionName, e.getKey()), new WALValue(UIO.longBytes(txId), timestamp, false));
                            highwaterUpdates.update(txId, -total);
                        }
                    }
                }
            }
        });
    }

    @Override
    public WALHighwater getRegionHighwater(RegionName regionName) throws Exception {
        WALKey from = walKey(regionName, null);
        WALKey to = from.prefixUpperExclusive();
        List<RingMemberHighwater> highwaters = new ArrayList<>();
        systemRegionStripe.rangeScan(RegionProvider.HIGHWATER_MARK_INDEX, from, to, (long rowTxId, WALKey key, WALValue value) -> {
            highwaters.add(new RingMemberHighwater(RingMember.fromBytes(key.getKey()), UIO.bytesLong(value.getValue())));
            return true;
        });
        return new WALHighwater(highwaters);
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
