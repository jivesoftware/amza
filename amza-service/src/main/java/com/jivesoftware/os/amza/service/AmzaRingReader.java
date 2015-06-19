package com.jivesoftware.os.amza.service;

import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.service.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.ring.RingNeighbors;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class AmzaRingReader {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingMember rootRingMember;
    private final PartitionIndex partitionIndex;

    public AmzaRingReader(RingMember rootRingMember, PartitionIndex partitionIndex) {
        this.rootRingMember = rootRingMember;
        this.partitionIndex = partitionIndex;
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
        return rootRingMember;
    }

    public RingNeighbors getRingNeighbors(String ringName) throws Exception {
        return new HostRingBuilder().build(rootRingMember, getRing(ringName));
    }

    public boolean isMemberOfRing(String ringName) throws Exception {
        PartitionStore ringIndex = partitionIndex.get(PartitionProvider.RING_INDEX);
        return ringIndex.containsKey(key(ringName, rootRingMember));
    }

    public NavigableMap<RingMember, RingHost> getRing(String ringName) throws Exception {
        PartitionStore ringIndex = partitionIndex.get(PartitionProvider.RING_INDEX);
        WALKey from = key(ringName, null);
        List<RingMember> ring = new ArrayList<>();
        ringIndex.rangeScan(from, from.prefixUpperExclusive(), (long orderId, WALKey key, WALValue value) -> {
            ring.add(keyToRingMember(key));
            return true;
        });

        PartitionStore nodeIndex = partitionIndex.get(PartitionProvider.NODE_INDEX);
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

    public RingHost getRingHost(RingMember ringMember) throws Exception {
        PartitionStore nodeIndex = partitionIndex.get(PartitionProvider.NODE_INDEX);
        WALValue[] rawRingHosts = nodeIndex.get(new WALKey[]{new WALKey(ringMember.toBytes())});
        return rawRingHosts[0] == null || rawRingHosts[0].getTombstoned() ? null : RingHost.fromBytes(rawRingHosts[0].getValue());
    }

    public Set<RingMember> getNeighboringRingMembers(String ringName) throws Exception {
        PartitionStore ringIndex = partitionIndex.get(PartitionProvider.RING_INDEX);
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

}
