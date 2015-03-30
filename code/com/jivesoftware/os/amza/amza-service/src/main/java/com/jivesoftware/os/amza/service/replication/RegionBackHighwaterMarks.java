package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RegionBackHighwaterMarks implements HighwaterMarks {

    private final OrderIdProvider orderIdProvider;
    private final RegionProvider regionProvider;
    private final int flushHighwatermarkAfterNUpdates;
    private final ConcurrentHashMap<RingHost, ConcurrentHashMap<RegionName, HighwaterMark>> hostHighwaterMarks = new ConcurrentHashMap<>();

    public RegionBackHighwaterMarks(OrderIdProvider orderIdProvider,
        RingHost rootHost,
        RegionProvider regionProvider,
        int flushHighwatermarkAfterNUpdates) {
        this.orderIdProvider = orderIdProvider;
        this.regionProvider = regionProvider;
        this.flushHighwatermarkAfterNUpdates = flushHighwatermarkAfterNUpdates;
    }

    WALKey walKey(RingHost ringHost, RegionName regionName) throws IOException {
        byte[] ringHostBytes = ringHost.toBytes();
        byte[] regionBytes = regionName.toBytes();
        MemoryFiler filer = new MemoryFiler();
        UIO.writeByte(filer, 0, "version");
        UIO.writeByteArray(filer, ringHostBytes, "host");
        UIO.writeByteArray(filer, regionBytes, "region");
        return new WALKey(filer.getBytes());
    }

    @Override
    public void setIfLarger(RingHost ringHost, RegionName regionName, int updates, long highwaterTxId) throws Exception {
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
        int total = highwaterMark.update(highwaterTxId, updates);
        if (total > flushHighwatermarkAfterNUpdates) {
            highwaterMark.update(highwaterTxId, -total);
            RowStoreUpdates rsu = regionProvider.getHighwaterIndexStore().startTransaction(orderIdProvider.nextId());
            rsu.add(walKey(ringHost, regionName), UIO.longBytes(highwaterMark.getTxId()));
            rsu.commit();
        }
    }

    @Override
    public void clear(RingHost ringHost, RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> regionHighwaterMarks = hostHighwaterMarks.get(ringHost);
        if (regionHighwaterMarks != null) {
            RowStoreUpdates rsu = regionProvider.getHighwaterIndexStore().startTransaction(orderIdProvider.nextId());
            rsu.remove(walKey(ringHost, regionName));
            rsu.commit();
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
            WALValue got = regionProvider.getHighwaterIndexStore().get(walKey(ringHost, regionName));
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
    public void clearRing(RingHost ringHost) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> regions = hostHighwaterMarks.get(ringHost);
        if (regions != null && !regions.isEmpty()) {
            RowStoreUpdates rsu = regionProvider.getHighwaterIndexStore().startTransaction(orderIdProvider.nextId());
            for (RegionName regionName : regions.keySet()) {
                rsu.remove(walKey(ringHost, regionName));
            }
            rsu.commit();
        }
        hostHighwaterMarks.remove(ringHost);
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
