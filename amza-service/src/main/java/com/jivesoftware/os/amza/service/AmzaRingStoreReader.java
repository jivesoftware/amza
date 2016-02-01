package com.jivesoftware.os.amza.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.RingMembership;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.api.value.ConcurrentBAHash;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.CacheId;
import com.jivesoftware.os.amza.service.ring.RingSet;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class AmzaRingStoreReader implements AmzaRingReader, RingMembership {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RingMember rootRingMember;
    private final PartitionStore ringIndex;
    private final PartitionStore nodeIndex;
    private final ConcurrentBAHash<CacheId<RingTopology>> ringsCache;
    private final ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache;
    private final AtomicLong nodeCacheId;

    public AmzaRingStoreReader(RingMember rootRingMember,
        PartitionStore ringIndex,
        PartitionStore nodeIndex,
        ConcurrentBAHash<CacheId<RingTopology>> ringsCache,
        ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache,
        AtomicLong nodeCacheId) {
        this.rootRingMember = rootRingMember;
        this.ringIndex = ringIndex;
        this.nodeIndex = nodeIndex;
        this.ringsCache = ringsCache;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
        this.nodeCacheId = nodeCacheId;
    }

    byte[] keyToRingName(WALKey walKey) throws IOException {
        HeapFiler filer = HeapFiler.fromBytes(walKey.key, walKey.key.length);
        return UIO.readByteArray(filer, "ringName", new byte[4]);
    }

    byte[] key(byte[] ringName, RingMember ringMember) throws Exception {
        HeapFiler filer = new HeapFiler();
        byte[] lengthBuffer = new byte[4];
        UIO.writeByteArray(filer, ringName, "ringName", lengthBuffer);
        UIO.write(filer, new byte[] { (byte) 0 }, "separator");
        if (ringMember != null) {
            byte[] ringMemberBytes = ringMember.toBytes();
            UIO.writeByteArray(filer, ringMemberBytes, "ringMember", lengthBuffer);
        }
        return filer.getBytes();
    }

    RingMember keyToRingMember(byte[] key) throws Exception {
        HeapFiler filer = HeapFiler.fromBytes(key, key.length);
        UIO.readByteArray(filer, "ringName", new byte[4]);
        UIO.readByte(filer, "separator");
        return RingMember.fromBytes(UIO.readByteArray(filer, "ringMember", new byte[4]));
    }

    @Override
    public RingMember getRingMember() {
        return rootRingMember;
    }

    public TimestampedRingHost getRingHost() throws Exception {
        TimestampedValue registeredHost = nodeIndex.getTimestampedValue(null, rootRingMember.toBytes());
        if (registeredHost != null) {
            return new TimestampedRingHost(RingHost.fromBytes(registeredHost.getValue()), registeredHost.getTimestampId());
        } else {
            return new TimestampedRingHost(RingHost.UNKNOWN_RING_HOST, -1);
        }
    }

    @Override
    public boolean isMemberOfRing(byte[] ringName) throws Exception {
        return getRing(ringName).rootMemberIndex >= 0;
    }

    @Override
    public RingTopology getRing(byte[] ringName) throws Exception {
        CacheId<RingTopology> cacheIdRingTopology = ringsCache.computeIfAbsent(ringName, key -> new CacheId<>(null));
        RingTopology ring = cacheIdRingTopology.entry;
        long currentRingCacheId = cacheIdRingTopology.currentCacheId;
        long currentNodeCacheId = nodeCacheId.get();
        if (ring == null || ring.ringCacheId != currentRingCacheId || ring.nodeCacheId != currentNodeCacheId) {
            try {
                List<RingMemberAndHost> orderedRing = Lists.newArrayList();
                int[] rootMemberIndex = {-1};
                byte[] from = key(ringName, null);
                nodeIndex.streamValues(null,
                    stream -> ringIndex.rangeScan(null,
                        from,
                        null,
                        WALKey.prefixUpperExclusive(from),
                        (rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                            if (!valueTombstone) {
                                RingMember ringMember = keyToRingMember(key);
                                return stream.stream(ringMember.toBytes());
                            } else {
                                return true;
                            }
                        }),
                    (rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                        RingMember ringMember = RingMember.fromBytes(key);
                        if (ringMember.equals(rootRingMember)) {
                            rootMemberIndex[0] = orderedRing.size();
                        }
                        if (value != null && !valueTombstone) {
                            orderedRing.add(new RingMemberAndHost(ringMember, RingHost.fromBytes(value)));
                        } else {
                            orderedRing.add(new RingMemberAndHost(ringMember, RingHost.UNKNOWN_RING_HOST));
                        }
                        return true;
                    });

                ring = new RingTopology(currentRingCacheId, currentNodeCacheId, orderedRing, rootMemberIndex[0]);
                cacheIdRingTopology.entry = ring;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return ring;
    }

    public void streamRingMembersAndHosts(RingMemberAndHostStream stream) throws Exception {
        nodeIndex.rowScan((rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            RingMember ringMember = RingMember.fromBytes(key);
            if (value != null && !valueTombstoned) {
                if (!stream.stream(new RingMemberAndHost(ringMember, RingHost.fromBytes(value)))) {
                    return false;
                }
            } else if (!stream.stream(new RingMemberAndHost(ringMember, RingHost.UNKNOWN_RING_HOST))) {
                return false;
            }
            return true;
        });

    }

    public static interface RingMemberAndHostStream {

        boolean stream(RingMemberAndHost ringMemberAndHost) throws Exception;
    }

    public RingHost getRingHost(RingMember ringMember) throws Exception {
        TimestampedValue rawRingHost = nodeIndex.getTimestampedValue(null, ringMember.toBytes());
        return rawRingHost == null ? null : RingHost.fromBytes(rawRingHost.getValue());
    }

    public Set<RingMember> getNeighboringRingMembers(byte[] ringName) throws Exception {
        byte[] from = key(ringName, null);
        Set<RingMember> ring = Sets.newHashSet();
        ringIndex.rangeScan(null,
            from,
            null,
            WALKey.prefixUpperExclusive(from),
            (rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
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
        CacheId<RingSet> cacheIdRingSet = ringMemberRingNamesCache.computeIfAbsent(desiredRingMember.leakBytes(), key -> new CacheId<>(null));
        RingSet ringSet = cacheIdRingSet.entry;
        long currentMemberCacheId = cacheIdRingSet.currentCacheId;
        if (ringSet == null || ringSet.memberCacheId != currentMemberCacheId) {
            try {
                ConcurrentBAHash<byte[]> ringNames = new ConcurrentBAHash<>(13, false, 1);
                try {
                    byte[] intBuffer = new byte[4];
                    ringIndex.rowScan((rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                        if (!valueTombstone) {
                            HeapFiler filer = HeapFiler.fromBytes(key, key.length);
                            byte[] ringName = UIO.readByteArray(filer, "ringName", intBuffer);
                            UIO.readByte(filer, "separator");
                            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "ringMember", intBuffer));
                            if (ringMember != null && ringMember.equals(desiredRingMember)) {
                                ringNames.put(ringName, ringName);
                            }
                        }
                        return true;
                    });
                } catch (Exception x) {
                    throw new RuntimeException(x);
                }

                ringSet = new RingSet(currentMemberCacheId, ringNames);
                cacheIdRingSet.entry = ringSet;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        ringSet.ringNames.stream((byte[] key, byte[] value) -> {
            return ringNameStream.stream(key);
        });
    }

    @Override
    public int getRingSize(byte[] ringName) throws Exception {
        return getRing(ringName).entries.size();
    }

    @Override
    public void allRings(RingStream ringStream) throws Exception {
        Map<RingMember, RingHost> ringMemberToRingHost = new HashMap<>();
        nodeIndex.rowScan((rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            if (!valueTombstone) {
                RingMember ringMember = RingMember.fromBytes(key);
                RingHost ringHost = RingHost.fromBytes(value);
                ringMemberToRingHost.put(ringMember, ringHost);
            }
            return true;
        });

        byte[] intBuffer = new byte[4];
        ringIndex.rowScan((rowType, prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            if (!valueTombstone) {
                HeapFiler filer = HeapFiler.fromBytes(key, key.length);
                byte[] ringName = UIO.readByteArray(filer, "ringName", intBuffer);
                UIO.readByte(filer, "separator");
                RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "ringMember", intBuffer));
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
