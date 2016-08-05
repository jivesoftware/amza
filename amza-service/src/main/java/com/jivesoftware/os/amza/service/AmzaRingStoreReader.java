package com.jivesoftware.os.amza.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.RingMembership;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.filer.HeapFiler;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.CacheId;
import com.jivesoftware.os.amza.service.ring.RingSet;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionStore;
import com.jivesoftware.os.jive.utils.collections.bah.BAHasher;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
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

    private final BAInterner interner;
    private final RingMember rootRingMember;
    private PartitionStore ringIndex;
    private PartitionStore nodeIndex;
    private final ConcurrentBAHash<CacheId<RingTopology>> ringsCache;
    private final ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache;
    private final AtomicLong nodeCacheId;
    private final Set<RingMember> blacklistRingMembers;

    public AmzaRingStoreReader(BAInterner interner,
        RingMember rootRingMember,
        ConcurrentBAHash<CacheId<RingTopology>> ringsCache,
        ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache,
        AtomicLong nodeCacheId,
        Set<RingMember> blacklistRingMembers) {
        this.interner = interner;
        this.rootRingMember = rootRingMember;
        this.ringsCache = ringsCache;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
        this.nodeCacheId = nodeCacheId;
        this.blacklistRingMembers = blacklistRingMembers;
    }

    public void start(PartitionIndex partitionIndex) throws Exception {
        ringIndex = partitionIndex.getSystemPartition(PartitionCreator.RING_INDEX);
        nodeIndex = partitionIndex.getSystemPartition(PartitionCreator.NODE_INDEX);
    }

    public void stop() {
        ringIndex = null;
        nodeIndex = null;
    }

    byte[] keyToRingName(WALKey walKey) throws IOException {
        HeapFiler filer = HeapFiler.fromBytes(walKey.key, walKey.key.length);
        return UIO.readByteArray(filer, "ringName", new byte[4]);
    }

    byte[] key(byte[] ringName, RingMember ringMember) throws Exception {

        byte[] key = new byte[4 + ringName.length + 1 + ((ringMember != null) ? 4 + ringMember.sizeInBytes() : 0)];
        int offset = 0;
        UIO.intBytes(ringName.length, key, offset);
        offset += 4;
        UIO.writeBytes(ringName, key, offset);
        offset += ringName.length;
        offset++; // separator
        if (ringMember != null) {
            UIO.intBytes(ringMember.sizeInBytes(), key, offset);
            offset += 4;
            offset += ringMember.toBytes(key, offset);
        }
        return key;
    }

    RingMember keyToRingMember(byte[] key) throws Exception {
        int o = 0;
        o += UIO.bytesInt(key, o); // ringName
        o += 4;
        o++; // separator
        int ringMemberLength = UIO.bytesInt(key, o);
        o += 4;
        return RingMember.fromBytes(key, o, ringMemberLength, interner);
    }

    @Override
    public RingMember getRingMember() {
        return rootRingMember;
    }

    public TimestampedRingHost getRingHost() throws Exception {
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }

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
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }

        CacheId<RingTopology> cacheIdRingTopology = ringsCache.computeIfAbsent(ringName, key -> new CacheId<>(null));
        RingTopology ring = cacheIdRingTopology.entry;
        long currentRingCacheId = cacheIdRingTopology.currentCacheId;
        long currentNodeCacheId = nodeCacheId.get();
        if (ring == null || ring.ringCacheId != currentRingCacheId || ring.nodeCacheId != currentNodeCacheId) {
            try {
                List<RingMemberAndHost> orderedRing = Lists.newArrayList();
                int[] rootMemberIndex = { -1 };
                byte[] from = key(ringName, null);
                nodeIndex.streamValues(null,
                    stream -> ringIndex.rangeScan(null,
                        from,
                        null,
                        WALKey.prefixUpperExclusive(from),
                        (prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                            if (!valueTombstone) {
                                RingMember ringMember = keyToRingMember(key);
                                if (blacklistRingMembers.contains(ringMember)) {
                                    return true;
                                } else {
                                    return stream.stream(ringMember.toBytes());
                                }
                            } else {
                                return true;
                            }
                        }, true),
                    (prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                        RingMember ringMember = RingMember.fromBytes(key, 0, key.length, interner);
                        if (!blacklistRingMembers.contains(ringMember)) {
                            if (ringMember.equals(rootRingMember)) {
                                rootMemberIndex[0] = orderedRing.size();
                            }
                            if (value != null && !valueTombstone) {
                                orderedRing.add(new RingMemberAndHost(ringMember, RingHost.fromBytes(value)));
                            } else {
                                orderedRing.add(new RingMemberAndHost(ringMember, RingHost.UNKNOWN_RING_HOST));
                            }
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
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }

        nodeIndex.rowScan((prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
            RingMember ringMember = RingMember.fromBytes(key, 0, key.length, interner);
            if (valueTombstoned || blacklistRingMembers.contains(ringMember)) {
                return true;
            } else if (value != null) {
                return stream.stream(new RingMemberAndHost(ringMember, RingHost.fromBytes(value)));
            } else {
                return stream.stream(new RingMemberAndHost(ringMember, RingHost.UNKNOWN_RING_HOST));
            }
        }, true);

    }

    public interface RingMemberAndHostStream {

        boolean stream(RingMemberAndHost ringMemberAndHost) throws Exception;
    }

    public RingHost getRingHost(RingMember ringMember) throws Exception {
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }

        TimestampedValue rawRingHost = nodeIndex.getTimestampedValue(null, ringMember.toBytes());
        return rawRingHost == null ? null : RingHost.fromBytes(rawRingHost.getValue());
    }

    public Set<RingMember> getNeighboringRingMembers(byte[] ringName) throws Exception {
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }

        byte[] from = key(ringName, null);
        Set<RingMember> ring = Sets.newHashSet();
        ringIndex.rangeScan(null,
            from,
            null,
            WALKey.prefixUpperExclusive(from),
            (prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                if (!valueTombstone) {
                    RingMember ringMember = keyToRingMember(key);
                    if (!blacklistRingMembers.contains(ringMember) && !ringMember.equals(rootRingMember)) {
                        ring.add(keyToRingMember(key));
                    }
                }
                return true;
            }, true);
        return ring;
    }

    @Override
    public void getRingNames(RingMember desiredRingMember, RingNameStream ringNameStream) throws Exception {
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }
        if (blacklistRingMembers.contains(desiredRingMember)) {
            throw new IllegalArgumentException("Requested ring member is blacklisted");
        }

        CacheId<RingSet> cacheIdRingSet = ringMemberRingNamesCache.computeIfAbsent(desiredRingMember.leakBytes(), key -> new CacheId<>(null));
        RingSet ringSet = cacheIdRingSet.entry;
        long currentMemberCacheId = cacheIdRingSet.currentCacheId;
        if (ringSet == null || ringSet.memberCacheId != currentMemberCacheId) {
            try {
                ConcurrentBAHash<Integer> ringNames = new ConcurrentBAHash<>(13, true, 1);
                try {
                    ringIndex.rowScan((prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                        if (!valueTombstone) {
                            int o = 0;
                            int ringNameLength = UIO.bytesInt(key, o);
                            o += 4;
                            byte[] ringName = interner.intern(key, o, ringNameLength);
                            o += ringNameLength;
                            o++; // separator
                            int ringMemberLength = UIO.bytesInt(key, o);
                            o += 4;
                            RingMember ringMember = RingMember.fromBytes(key, o, ringMemberLength, interner);
                            if (ringMember != null && ringMember.equals(desiredRingMember)) {
                                ringNames.put(ringName, BAHasher.SINGLETON.hashCode(ringName, 0, ringName.length));
                            }
                        }
                        return true;
                    }, true);
                } catch (Exception x) {
                    throw new RuntimeException(x);
                }

                ringSet = new RingSet(currentMemberCacheId, ringNames);
                cacheIdRingSet.entry = ringSet;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        ringSet.ringNames.stream(ringNameStream::stream);
    }

    @Override
    public int getRingSize(byte[] ringName) throws Exception {
        return getRing(ringName).entries.size();
    }

    @Override
    public int getTakeFromFactor(byte[] ringName) throws Exception {
        return getRing(ringName).getTakeFromFactor();
    }

    @Override
    public void allRings(RingStream ringStream) throws Exception {
        if (ringIndex == null || nodeIndex == null) {
            throw new IllegalStateException("Ring store reader wasn't opened or has already been closed.");
        }
        Map<RingMember, RingHost> ringMemberToRingHost = new HashMap<>();
        nodeIndex.rowScan((prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            if (!valueTombstone) {
                RingMember ringMember = RingMember.fromBytes(key, 0, key.length, interner);
                if (!blacklistRingMembers.contains(ringMember)) {
                    RingHost ringHost = RingHost.fromBytes(value);
                    ringMemberToRingHost.put(ringMember, ringHost);
                }
            }
            return true;
        }, true);

        ringIndex.rowScan((prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
            if (!valueTombstone) {
                int o = 0;
                int ringNameLength = UIO.bytesInt(key, o);
                o += 4;
                byte[] ringName = interner.intern(key, o, ringNameLength);
                o += ringNameLength;
                o++; // separator
                int ringMemberLength = UIO.bytesInt(key, o);
                o += 4;
                RingMember ringMember = RingMember.fromBytes(key, o, ringMemberLength, interner);
                if (blacklistRingMembers.contains(ringMember)) {
                    return true;
                } else {
                    RingHost ringHost = ringMemberToRingHost.get(ringMember);
                    if (ringHost == null) {
                        ringHost = RingHost.UNKNOWN_RING_HOST;
                    }
                    return ringStream.stream(ringName, ringMember, ringHost);
                }
            } else {
                return true;
            }
        }, true);
    }

}
