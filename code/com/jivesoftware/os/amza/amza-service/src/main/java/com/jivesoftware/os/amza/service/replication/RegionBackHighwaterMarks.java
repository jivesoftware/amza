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

public class RegionBackHighwaterMarks implements HighwaterMarks {

    private final OrderIdProvider orderIdProvider;
    private final RingHost rootHost;
    private final RegionProvider regionProvider;
    private final int flushHighwatermarkAfterNUpdates;
    private final ConcurrentHashMap<RingHost, ConcurrentHashMap<RegionName, HighwaterMark>> lastTransactionIds = new ConcurrentHashMap<>();

    public RegionBackHighwaterMarks(OrderIdProvider orderIdProvider,
        RingHost rootHost,
        RegionProvider regionProvider,
        int flushHighwatermarkAfterNUpdates) {
        this.orderIdProvider = orderIdProvider;
        this.rootHost = rootHost;
        this.regionProvider = regionProvider;
        this.flushHighwatermarkAfterNUpdates = flushHighwatermarkAfterNUpdates;
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
    public void clearRing(RingHost ringHost) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> regions = lastTransactionIds.get(ringHost);
        if (regions != null && !regions.isEmpty()) {
            RowStoreUpdates rsu = regionProvider.getHighwaterIndexStore().startTransaction(orderIdProvider.nextId());
            for (RegionName regionName : regions.keySet()) {
                rsu.remove(walKey(ringHost, regionName));
            }
            rsu.commit();
        }
        lastTransactionIds.remove(ringHost);
    }

    @Override
    public void set(RingHost ringHost, RegionName regionName, int updates, long highwaterTxId) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> lastRegionTransactionIds = lastTransactionIds.get(ringHost);
        if (lastRegionTransactionIds == null) {
            lastRegionTransactionIds = new ConcurrentHashMap<>();
            lastTransactionIds.put(ringHost, lastRegionTransactionIds);
        }
        HighwaterMark highwaterMark = new HighwaterMark(highwaterTxId);
        HighwaterMark had = lastRegionTransactionIds.putIfAbsent(regionName, highwaterMark);
        if (had != null) {
            highwaterMark = had;
        }
        int total = highwaterMark.update(updates);
        if (total > flushHighwatermarkAfterNUpdates) {
            highwaterMark.update(-total);
            RowStoreUpdates rsu = regionProvider.getHighwaterIndexStore().startTransaction(orderIdProvider.nextId());
            rsu.add(walKey(ringHost, regionName), UIO.longBytes(highwaterMark.txId));
            rsu.commit();
        }
    }

    @Override
    public void clear(RingHost ringHost, RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> lastRegionTransactionIds = lastTransactionIds.get(ringHost);
        if (lastRegionTransactionIds != null) {
            RowStoreUpdates rsu = regionProvider.getHighwaterIndexStore().startTransaction(orderIdProvider.nextId());
            rsu.remove(walKey(ringHost, regionName));
            rsu.commit();
            lastRegionTransactionIds.remove(regionName);
        }
    }

    @Override
    public Long get(RingHost ringHost, RegionName regionName) throws Exception {
        ConcurrentHashMap<RegionName, HighwaterMark> lastRegionTransactionIds = lastTransactionIds.get(ringHost);
        if (lastRegionTransactionIds == null) {
            WALValue got = regionProvider.getHighwaterIndexStore().get(walKey(ringHost, regionName));
            if (got != null) {
                lastRegionTransactionIds = new ConcurrentHashMap<>();
                ConcurrentHashMap<RegionName, HighwaterMark> had = lastTransactionIds.putIfAbsent(ringHost, lastRegionTransactionIds);
                if (had != null) {
                    lastRegionTransactionIds = had;
                }
                long highwaterTxId = UIO.bytesLong(got.getValue());
                lastRegionTransactionIds.put(regionName, new HighwaterMark(highwaterTxId));
                return highwaterTxId;
            }
            return -1L;
        }
        HighwaterMark highwaterMark = lastRegionTransactionIds.get(regionName);
        if (highwaterMark == null) {
            return -1L;
        }
        return highwaterMark.getTxId();
    }

    static class HighwaterMark {

        private final long txId;
        private final AtomicInteger updates = new AtomicInteger(0);

        public HighwaterMark(long txId) {
            this.txId = txId;
        }

        public int update(int updates) {
            return this.updates.addAndGet(updates);
        }

        public long getTxId() {
            return txId;
        }

    }
}
