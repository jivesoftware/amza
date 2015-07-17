package com.jivesoftware.os.amza.service;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private final ConcurrentMap<IBA, Integer> ringSizesCache;
    private final ConcurrentMap<RingMember, Set<IBA>> ringMemberRingNamesCache;

    public AmzaRingStoreReader(RingMember rootRingMember,
        PartitionStore ringIndex,
        PartitionStore nodeIndex,
        ConcurrentMap<IBA, Integer> ringSizesCache,
        ConcurrentMap<RingMember, Set<IBA>> ringMemberRingNamesCache) {
        this.rootRingMember = rootRingMember;
        this.ringIndex = ringIndex;
        this.nodeIndex = nodeIndex;
        this.ringSizesCache = ringSizesCache;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
    }

    byte[] keyToRingName(WALKey key) throws IOException {
        HeapFiler filer = new HeapFiler(key.getKey());
        return UIO.readByteArray(filer, "ringName");
    }

    byte[] key(byte[] ringName, RingMember ringMember) throws Exception {
        HeapFiler filer = new HeapFiler();
        UIO.writeByteArray(filer, ringName, "ringName");
        UIO.writeByte(filer, 0, "seperator");
        if (ringMember != null) {
            UIO.writeByteArray(filer, ringMember.toBytes(), "ringMember");
        }
        return filer.getBytes();
    }

    RingMember keyToRingMember(byte[] key) throws Exception {
        HeapFiler filer = new HeapFiler(key);
        UIO.readByteArray(filer, "ringName");
        UIO.readByte(filer, "seperator");
        return RingMember.fromBytes(UIO.readByteArray(filer, "ringMember"));
    }

    @Override
    public RingMember getRingMember() {
        return rootRingMember;
    }

    @Override
    public List<Entry<RingMember, RingHost>> getNeighbors(byte[] ringName) throws Exception {
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

    public boolean isMemberOfRing(byte[] ringName) throws Exception {
        boolean[] isMember = new boolean[1];
        ringIndex.containsKeys(stream -> stream.stream(key(ringName, rootRingMember)),
            (key, contained) -> {
                isMember[0] = contained;
                return true;
            });
        return isMember[0];
    }

    @Override
    public NavigableMap<RingMember, RingHost> getRing(byte[] ringName) throws Exception {

        NavigableMap<RingMember, RingHost> orderedRing = new TreeMap<>();
        byte[] from = key(ringName, null);
        nodeIndex.get(
            stream -> ringIndex.rangeScan(from,
                WALKey.prefixUpperExclusive(from),
                (key, value, valueTimestamp, valueTombstone) -> {
                    if (!valueTombstone) {
                        RingMember ringMember = keyToRingMember(key);
                        return stream.stream(ringMember.toBytes());
                    } else {
                        return true;
                    }
                }),
            (key, value, valueTimestamp, valueTombstone) -> {
                if (value != null && !valueTombstone) {
                    orderedRing.put(RingMember.fromBytes(key), RingHost.fromBytes(value));
                } else {
                    orderedRing.put(RingMember.fromBytes(key), RingHost.UNKNOWN_RING_HOST);
                }
                return true;
            });
        return orderedRing;
    }

    public RingHost getRingHost(RingMember ringMember) throws Exception {
        TimestampedValue rawRingHost = nodeIndex.get(ringMember.toBytes());
        return rawRingHost == null ? null : RingHost.fromBytes(rawRingHost.getValue());
    }

    public Set<RingMember> getNeighboringRingMembers(byte[] ringName) throws Exception {
        byte[] from = key(ringName, null);
        Set<RingMember> ring = Sets.newHashSet();
        ringIndex.rangeScan(from, WALKey.prefixUpperExclusive(from), (key, value, valueTimestamp, valueTombstone) -> {
            if (!valueTombstone) {
                RingMember ringMember = keyToRingMember(key);
                if (!ringMember.equals(rootRingMember)) {
                    ring.add(keyToRingMember(key));
                }
            }
            return true;
        });
        return ring;
    }

    @Override
    public void getRingNames(RingMember desiredRingMember, RingNameStream ringNameStream) throws Exception {

        Set<IBA> ringNames = ringMemberRingNamesCache.computeIfAbsent(desiredRingMember, (key) -> {
            Set<IBA> set = new HashSet<>();
            try {
                ringIndex.rowScan((walKey, value, valueTimestamp, valueTombstone) -> {
                    if (!valueTombstone) {
                        HeapFiler filer = new HeapFiler(walKey);
                        byte[] ringName = UIO.readByteArray(filer, "ringName");
                        UIO.readByte(filer, "seperator");
                        RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "ringMember"));
                        if (ringMember != null && ringMember.equals(desiredRingMember)) {
                            set.add(new IBA(ringName));
                        }
                    }
                    return true;
                });
                return set;
            } catch (Exception x) {
                throw new RuntimeException(x);
            }
        });
        for (IBA ringName : ringNames) {
            ringNameStream.stream(ringName.getBytes());
        }
    }

    @Override
    public int getRingSize(byte[] ringName) throws Exception {
        return ringSizesCache.computeIfAbsent(new IBA(ringName), key -> {
            try {
                return getRing(key.getBytes()).size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void allRings(RingStream ringStream) throws Exception {
        Map<RingMember, RingHost> ringMemberToRingHost = new HashMap<>();
        nodeIndex.rowScan((key, value, valueTimestamp, valueTombstone) -> {
            if (!valueTombstone) {
                RingMember ringMember = RingMember.fromBytes(key);
                RingHost ringHost = RingHost.fromBytes(value);
                ringMemberToRingHost.put(ringMember, ringHost);
            }
            return true;
        });

        ringIndex.rowScan((key, value, valueTimestamp, valueTombstone) -> {
            if (!valueTombstone) {
                HeapFiler filer = new HeapFiler(key);
                byte[] ringName = UIO.readByteArray(filer, "ringName");
                UIO.readByte(filer, "seperator");
                RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "ringMember"));
                RingHost ringHost = ringMemberToRingHost.get(ringMember);
                if (ringHost == null) {
                    ringHost = RingHost.UNKNOWN_RING_HOST;
                }
                return ringStream.stream(ringName, ringMember, ringHost);
            } else {
                return true;
            }
        });
    }

}
