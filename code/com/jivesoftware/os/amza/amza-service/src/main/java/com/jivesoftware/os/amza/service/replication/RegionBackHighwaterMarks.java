package com.jivesoftware.os.amza.service.replication;

import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RegionBackHighwaterMarks implements HighwaterMarks {

    private final OrderIdProvider orderIdProvider;
    private final RingHost rootHost;
    private final RegionStripe systemRegionStripe;
    private final WALReplicator replicator;
    private final ConcurrentHashMap<RingHost, ConcurrentHashMap<RegionName, HighwaterMark>> hostHighwaterMarks = new ConcurrentHashMap<>();

    public RegionBackHighwaterMarks(OrderIdProvider orderIdProvider,
        RingHost rootHost,
        RegionStripe systemRegionStripe,
        WALReplicator replicator) {
        this.orderIdProvider = orderIdProvider;
        this.rootHost = rootHost;
        this.systemRegionStripe = systemRegionStripe;
        this.replicator = replicator;
    }

    WALKey walKey(RingHost ringHost, RegionName regionName) throws IOException {
        byte[] rootHostBytes = rootHost.toBytes();
        byte[] ringHostBytes = ringHost.toBytes();
        byte[] regionBytes = regionName.toBytes();
        MemoryFiler filer = new MemoryFiler();
        UIO.writeByte(filer, 0, "version");
        UIO.writeByteArray(filer, rootHostBytes, "rootHost");
        UIO.writeByteArray(filer, ringHostBytes, "host");
        UIO.writeByteArray(filer, regionBytes, "region");
        return new WALKey(filer.getBytes());
    }

    @Override
    public void setIfLarger(final RingHost ringHost, final RegionName regionName, int updates, long highwaterTxId) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> regionHighwaterMarks = hostHighwaterMarks.get(ringHost);
        if (regionHighwaterMarks == null) {
            regionHighwaterMarks = new ConcurrentHashMap<>();
            ConcurrentHashMap<RegionName, HighwaterMark> had = hostHighwaterMarks.putIfAbsent(ringHost, regionHighwaterMarks);
            if (had != null) {
                regionHighwaterMarks = had;
            }
        }
        HighwaterMark highwaterMark = new HighwaterMark();
        HighwaterMark had = regionHighwaterMarks.putIfAbsent(regionName, highwaterMark);
        if (had != null) {
            highwaterMark = had;
        }
        highwaterMark.update(highwaterTxId, updates);
    }

    @Override
    public void clear(final RingHost ringHost, final RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> regionHighwaterMarks = hostHighwaterMarks.get(ringHost);
        if (regionHighwaterMarks != null) {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (Scan<WALValue> scan) -> {
                scan.row(-1, walKey(ringHost, regionName), new WALValue(null, orderIdProvider.nextId(), true));
            });
            regionHighwaterMarks.remove(regionName);
        }
    }

    @Override
    public Long get(RingHost ringHost, RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> regionHighwaterMarks = hostHighwaterMarks.get(ringHost);
        if (regionHighwaterMarks == null) {
            regionHighwaterMarks = new ConcurrentHashMap<>();
            ConcurrentHashMap<RegionName, HighwaterMark> had = hostHighwaterMarks.putIfAbsent(ringHost, regionHighwaterMarks);
            if (had != null) {
                regionHighwaterMarks = had;
            }
        }
        HighwaterMark highwaterMark = regionHighwaterMarks.get(regionName);
        if (highwaterMark == null) {
            WALValue got = systemRegionStripe.get(RegionProvider.HIGHWATER_MARK_INDEX, walKey(ringHost, regionName));
            long txtId = -1L;
            if (got != null) {
                txtId = UIO.bytesLong(got.getValue());
            }
            highwaterMark = new HighwaterMark();
            HighwaterMark hadHighwaterMark = regionHighwaterMarks.putIfAbsent(regionName, highwaterMark);
            if (hadHighwaterMark != null) {
                highwaterMark = hadHighwaterMark;
            }
            highwaterMark.update(txtId, 0);
        }
        return highwaterMark.getTxId();
    }

    @Override
    public void clearRing(final RingHost ringHost) throws Exception {
        final ConcurrentHashMap<RegionName, HighwaterMark> regions = hostHighwaterMarks.get(ringHost);
        if (regions != null && !regions.isEmpty()) {
            systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (Scan<WALValue> scan) -> {
                long timestamp = orderIdProvider.nextId();
                for (RegionName regionName : regions.keySet()) {
                    scan.row(-1, walKey(ringHost, regionName), new WALValue(null, timestamp, true));
                }
            });

        }
        hostHighwaterMarks.remove(ringHost);
    }

    @Override
    public void flush(final ListMultimap<RingHost, RegionName> flush) throws Exception {

        systemRegionStripe.commit(RegionProvider.HIGHWATER_MARK_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (Scan<WALValue> scan) -> {
            long timestamp = orderIdProvider.nextId();
            for (Entry<RingHost, Collection<RegionName>> e : flush.asMap().entrySet()) {
                final ConcurrentHashMap<RegionName, HighwaterMark> highwaterMarks = hostHighwaterMarks.get(e.getKey());
                if (highwaterMarks != null) {
                    for (RegionName regionName : e.getValue()) {
                        HighwaterMark highwaterMark = highwaterMarks.get(regionName);
                        if (highwaterMark != null && highwaterMark.updates.get() > 0) {
                            long txId = highwaterMark.getTxId();
                            int total = highwaterMark.updates.get();
                            scan.row(-1, walKey(e.getKey(), regionName), new WALValue(UIO.longBytes(txId), timestamp, false));
                            highwaterMark.update(txId, -total);
                        }
                    }
                }
            }
        });
    }

    static class HighwaterMark {

        private final AtomicLong txId;
        private final AtomicInteger updates = new AtomicInteger(0);

        public HighwaterMark() {
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
