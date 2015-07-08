package com.jivesoftware.os.amza.service;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.wal.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

public class AmzaRingStoreReader implements AmzaRingReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingMember rootRingMember;
    private final PartitionStore ringIndex;
    private final PartitionStore nodeIndex;
    private final ConcurrentMap<String, Integer> ringSizesCache;
    private final ConcurrentMap<RingMember, Set<String>> ringMemberRingNamesCache;

    public AmzaRingStoreReader(RingMember rootRingMember,
        PartitionStore ringIndex,
        PartitionStore nodeIndex,
        ConcurrentMap<String, Integer> ringSizesCache,
        ConcurrentMap<RingMember, Set<String>> ringMemberRingNamesCache) {
        this.rootRingMember = rootRingMember;
        this.ringIndex = ringIndex;
        this.nodeIndex = nodeIndex;
        this.ringSizesCache = ringSizesCache;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
    }

    String keyToRingName(WALKey key) throws IOException {
        HeapFiler filer = new HeapFiler(key.getKey());
        return UIO.readString(filer, "ringName");
    }

    WALKey key(String ringName, RingMember ringMember) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeString(filer, ringName.toUpperCase(Locale.US), "ringName");
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

    @Override
    public RingMember getRingMember() {
        return rootRingMember;
    }

    @Override
    public List<Entry<RingMember, RingHost>> getNeighbors(String ringName) throws Exception {
        NavigableMap<RingMember, RingHost> ring = getRing(ringName);
        if (!ring.containsKey(rootRingMember) || ring.size() < 2) {
            return Collections.emptyList();
        } else {
            List<Entry<RingMember, RingHost>> neighbors = new ArrayList<>(ring.size() - 1);
            ring.tailMap(rootRingMember, false).forEach((RingMember k, RingHost v) -> {
                neighbors.add(new SimpleEntry<>(k, v));
            });
            ring.headMap(rootRingMember, false).forEach((RingMember k, RingHost v) -> {
                neighbors.add(new SimpleEntry<>(k, v));
            });
            return neighbors;
        }

    }

    public boolean isMemberOfRing(String ringName) throws Exception {
        return ringIndex.containsKey(key(ringName, rootRingMember));
    }

    @Override
    public NavigableMap<RingMember, RingHost> getRing(String ringName) throws Exception {

        NavigableMap<RingMember, RingHost> orderedRing = new TreeMap<>();
        WALKey from = key(ringName, null);
        nodeIndex.get((KeyValueStream stream) -> {
            ringIndex.rangeScan(from, from.prefixUpperExclusive(), (long orderId, WALKey key, WALValue value) -> {
                RingMember ringMember = keyToRingMember(key);
                return stream.stream(new WALKey(ringMember.toBytes()), null);
            });
        }, (WALKey key, WALValue value) -> {
            if (value != null && !value.getTombstoned()) {
                orderedRing.put(RingMember.fromBytes(key.getKey()), RingHost.fromBytes(value.getValue()));
            } else {
                orderedRing.put(RingMember.fromBytes(key.getKey()), RingHost.UNKNOWN_RING_HOST);
            }
            return true;
        });
        return orderedRing;
    }

    public RingHost getRingHost(RingMember ringMember) throws Exception {
        WALValue rawRingHost = nodeIndex.get(new WALKey(ringMember.toBytes()));
        return rawRingHost == null || rawRingHost.getTombstoned() ? null : RingHost.fromBytes(rawRingHost.getValue());
    }

    public Set<RingMember> getNeighboringRingMembers(String ringName) throws Exception {
        WALKey from = key(ringName, null);
        Set<RingMember> ring = Sets.newHashSet();
        ringIndex.rangeScan(from, from.prefixUpperExclusive(), (long orderId, WALKey key, WALValue value) -> {
            RingMember ringMember = keyToRingMember(key);
            if (!ringMember.equals(rootRingMember)) {
                ring.add(keyToRingMember(key));
            }
            return true;
        });
        return ring;
    }

    @Override
    public void getRingNames(RingMember desiredRingMember, RingNameStream ringNameStream) throws Exception {

        Set<String> ringNames = ringMemberRingNamesCache.computeIfAbsent(desiredRingMember, (key) -> {
            Set<String> set = new HashSet<>();
            try {
                ringIndex.rowScan((long rowTxId, WALKey walKey, WALValue value) -> {
                    HeapFiler filer = new HeapFiler(walKey.getKey());
                    String ringName = UIO.readString(filer, "ringName");
                    UIO.readByte(filer, "seperator");
                    RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "ringMember"));
                    if (ringMember.equals(desiredRingMember)) {
                        set.add(ringName);
                    }
                    return true;
                });
                return set;
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        });
        for (String ringName : ringNames) {
            ringNameStream.stream(ringName);
        }
    }

    @Override
    public int getRingSize(String ringName) throws Exception {
        return ringSizesCache.computeIfAbsent(ringName, key -> {
            try {
                return getRing(key).size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void allRings(RingStream ringStream) throws Exception {
        Map<RingMember, RingHost> ringMemberToRingHost = new HashMap<>();
        nodeIndex.rowScan((long rowTxId, WALKey key, WALValue rawRingHost) -> {
            RingMember ringMember = RingMember.fromBytes(key.getKey());
            RingHost ringHost = RingHost.fromBytes(rawRingHost.getValue());
            ringMemberToRingHost.put(ringMember, ringHost);
            return true;
        });

        ringIndex.rowScan((long rowTxId, WALKey key, WALValue value) -> {
            HeapFiler filer = new HeapFiler(key.getKey());
            String ringName = UIO.readString(filer, "ringName");
            UIO.readByte(filer, "seperator");
            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "ringMember"));
            RingHost ringHost = ringMemberToRingHost.get(ringMember);
            if (ringHost == null) {
                ringHost = RingHost.UNKNOWN_RING_HOST;
            }
            return ringStream.stream(ringName, ringMember, ringHost);
        });
    }

}
