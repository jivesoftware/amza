package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class AmzaRingReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingMember ringMember;
    private final RegionIndex regionIndex;

    public AmzaRingReader(RingMember ringMember, RegionIndex regionIndex) {
        this.ringMember = ringMember;
        this.regionIndex = regionIndex;
    }

    String keyToRingName(WALKey key) throws IOException {
        HeapFiler filer = new HeapFiler(key.getKey());
        return UIO.readString(filer, "ringName");
    }

    WALKey key(String ringName, RingMember ringMember) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeString(filer, ringName.toUpperCase(), "ringName");
        UIO.writeByte(filer, 0, "seperator");
        if (ringMember != null) {
            UIO.writeByteArray(filer, ringMember.toBytes(), "ringMember");
        }
        return new WALKey(filer.getBytes());
    }

    RingMember keyToRingMember(WALKey key) throws IOException {
        HeapFiler filer = new HeapFiler(key.getKey());
        UIO.readString(filer, "ringName");
        UIO.readByte(filer, "seperator");
        return RingMember.fromBytes(UIO.readByteArray(filer, "ringMember"));
    }

    public RingMember getRingMember() {
        return ringMember;
    }

    public RingNeighbors getRingNeighbors(String ringName) throws Exception {
        return new HostRingBuilder().build(ringMember, getRing(ringName));
    }

    public boolean isMemberOfRing(String ringName) throws Exception {
        RegionStore ringIndex = regionIndex.get(RegionProvider.RING_INDEX);
        return ringIndex.containsKey(key(ringName, ringMember));
    }

    public NavigableMap<RingMember, RingHost> getRing(String ringName) throws Exception {
        RegionStore ringIndex = regionIndex.get(RegionProvider.RING_INDEX);
        WALKey from = key(ringName, null);
        List<RingMember> ring = new ArrayList<>();
        ringIndex.rangeScan(from, from.prefixUpperExclusive(), (long orderId, WALKey key, WALValue value) -> {
            ring.add(keyToRingMember(key));
            return true;
        });

        RegionStore nodeIndex = regionIndex.get(RegionProvider.NODE_INDEX);
        WALKey[] memberKeys = new WALKey[ring.size()];
        for (int i = 0; i < memberKeys.length; i++) {
            memberKeys[i] = new WALKey(ring.get(i).toBytes());
        }
        WALValue[] rawRingHosts = nodeIndex.get(memberKeys);
        NavigableMap<RingMember, RingHost> orderedRing = new TreeMap<>();
        for (int i = 0; i < rawRingHosts.length; i++) {
            WALValue rawRingHost = rawRingHosts[i];
            if (rawRingHost != null && !rawRingHost.getTombstoned()) {
                orderedRing.put(ring.get(i), RingHost.fromBytes(rawRingHost.getValue()));
            } else {
                orderedRing.put(ring.get(i), RingHost.UNKNOWN_RING_HOST);
            }
        }
        return orderedRing;
    }

}
