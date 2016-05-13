/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.scan.RowChanges;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.service.ring.CacheId;
import com.jivesoftware.os.amza.service.ring.RingSet;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class AmzaRingStoreWriter implements AmzaRingWriter, RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRingStoreReader ringStoreReader;
    private final SystemWALStorage systemWALStorage;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final WALUpdated walUpdated;
    private final ConcurrentBAHash<CacheId<RingTopology>> ringsCache;
    private final ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache;
    private final AtomicLong nodeCacheId;
    private final boolean rackDistributionEnabled;

    public AmzaRingStoreWriter(AmzaRingStoreReader ringStoreReader,
        SystemWALStorage systemWALStorage,
        TimestampedOrderIdProvider orderIdProvider,
        WALUpdated walUpdated,
        ConcurrentBAHash<CacheId<RingTopology>> ringsCache,
        ConcurrentBAHash<CacheId<RingSet>> ringMemberRingNamesCache,
        AtomicLong nodeCacheId,
        boolean rackDistributionEnabled) {
        this.ringStoreReader = ringStoreReader;
        this.systemWALStorage = systemWALStorage;
        this.orderIdProvider = orderIdProvider;
        this.walUpdated = walUpdated;
        this.ringsCache = ringsCache;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
        this.nodeCacheId = nodeCacheId;
        this.rackDistributionEnabled = rackDistributionEnabled;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.RING_INDEX.equals(changes.getVersionedPartitionName())) {
            for (WALKey walKey : changes.getApply().keySet()) {
                byte[] ringBytes = ringStoreReader.keyToRingName(walKey);
                ringsCache.compute(ringBytes, (key, cacheIdRingTopology) -> {
                    if (cacheIdRingTopology == null) {
                        cacheIdRingTopology = new CacheId<>(null);
                    }
                    cacheIdRingTopology.currentCacheId++;
                    /*LOG.info("Rings advanced {} to {}", Arrays.toString(ringBytes), cacheIdRingTopology.currentCacheId);*/
                    return cacheIdRingTopology;
                });
                RingMember ringMember = ringStoreReader.keyToRingMember(walKey.key);
                ringMemberRingNamesCache.compute(ringMember.leakBytes(), (ringMember1, cacheIdRingSet) -> {
                    if (cacheIdRingSet == null) {
                        cacheIdRingSet = new CacheId<>(null);
                    }
                    cacheIdRingSet.currentCacheId++;
                    return cacheIdRingSet;
                });
            }
        } else if (PartitionCreator.NODE_INDEX.equals(changes.getVersionedPartitionName())) {
            if (!changes.getApply().isEmpty()) {
                nodeCacheId.incrementAndGet();
                /*LOG.info("Node advanced to {}", nodeCacheId.get());*/
            }
        }
    }

    @Override
    public void register(RingMember ringMember, RingHost ringHost, long timestampId) throws Exception {
        TimestampedValue registeredHost = systemWALStorage.getTimestampedValue(PartitionCreator.NODE_INDEX, null, ringMember.toBytes());
        if (registeredHost == null || !ringHost.equals(RingHost.fromBytes(registeredHost.getValue()))) {
            long version = orderIdProvider.nextId();
            long timestamp = (timestampId == -1) ? version : timestampId;
            systemWALStorage.update(PartitionCreator.NODE_INDEX, null,
                (highwater, scan) -> scan.row(-1, ringMember.toBytes(), ringHost.toBytes(), timestamp, false, version),
                walUpdated);
            LOG.info("register ringMember:{} as ringHost:{}", ringMember, ringHost);
        }

        addRingMember(AmzaRingReader.SYSTEM_RING, ringMember);
    }

    @Override
    public void deregister(RingMember ringMember) throws Exception {
        removeRingMember(AmzaRingReader.SYSTEM_RING, ringMember);

        long timestampAndVersion = orderIdProvider.nextId();
        systemWALStorage.update(PartitionCreator.NODE_INDEX, null,
            (highwater, scan) -> scan.row(-1, ringMember.toBytes(), null, timestampAndVersion, true, timestampAndVersion),
            walUpdated);
        LOG.info("deregister ringMember:{}");
    }

    public boolean isMemberOfRing(byte[] ringName) throws Exception {
        return ringStoreReader.isMemberOfRing(ringName);
    }

    @Override
    public void ensureMaximalRing(byte[] ringName) throws Exception {
        ensureSubRing(ringName, ringStoreReader.getRingSize(AmzaRingReader.SYSTEM_RING));
    }

    @Override
    public void ensureSubRing(byte[] ringName, int desiredRingSize) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        int ringSize = ringStoreReader.getRingSize(ringName);
        if (ringSize < desiredRingSize) {
            LOG.info("Ring {} will grow, has {} desires {}", ringName, ringSize, desiredRingSize);
            buildRandomSubRing(ringName, desiredRingSize);
        }
    }

    private void buildRandomSubRing(byte[] ringName, int desiredRingSize) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        RingTopology systemRing = ringStoreReader.getRing(AmzaRingReader.SYSTEM_RING);
        if (systemRing.entries.size() < desiredRingSize) {
            throw new IllegalStateException("Current 'system' ring is not large enough to support a ring of size:" + desiredRingSize);
        }

        SetMultimap<String, RingMemberAndHost> subRackMembers = HashMultimap.create();
        RingTopology subRing = ringStoreReader.getRing(ringName);
        for (RingMemberAndHost entry : subRing.entries) {
            String rack = rackDistributionEnabled ? entry.ringHost.getRack() : "";
            subRackMembers.put(rack, entry);
        }

        Map<String, List<RingMemberAndHost>> systemRackMembers = new HashMap<>();
        for (RingMemberAndHost entry : systemRing.entries) {
            String rack = rackDistributionEnabled ? entry.ringHost.getRack() : "";
            systemRackMembers.computeIfAbsent(rack, (key) -> new ArrayList<>()).add(entry);
        }

        Random random = new Random(new Random(Arrays.hashCode(ringName)).nextLong());
        for (List<RingMemberAndHost> rackMembers : systemRackMembers.values()) {
            Collections.shuffle(rackMembers, random);
        }

        List<String> racks = new ArrayList<>(systemRackMembers.keySet());

        while (subRackMembers.size() < desiredRingSize) {
            Collections.sort(racks, (o1, o2) -> Integer.compare(subRackMembers.get(o1).size(), subRackMembers.get(o2).size()));
            boolean advanced = false;
            for (String cycleRack : racks) {
                List<RingMemberAndHost> rackMembers = systemRackMembers.get(cycleRack);
                if (!rackMembers.isEmpty()) {
                    subRackMembers.put(cycleRack, rackMembers.remove(rackMembers.size() - 1));
                    advanced = true;
                    break;
                }
            }
            if (!advanced) {
                break;
            }
        }

        setInternal(ringName, Iterables.transform(subRackMembers.values(), input -> input.ringMember));
    }

    private static long hash(byte[] bytes) {
        long hashed = (long) Arrays.hashCode(bytes) << 32;
        for (int i = 0; i < bytes.length; i++) {
            hashed ^= (bytes[i] & 0xFF) << i;
        }
        return hashed;
    }

    @Override
    public void addRingMember(byte[] ringName, RingMember ringMember) throws Exception {
        Preconditions.checkNotNull(ringName, "ringName cannot be null.");
        Preconditions.checkNotNull(ringMember, "ringMember cannot be null.");
        byte[] key = ringStoreReader.key(ringName, ringMember);
        TimestampedValue had = systemWALStorage.getTimestampedValue(PartitionCreator.RING_INDEX, null, key);
        if (had == null) {
            RingTopology ring = ringStoreReader.getRing(ringName);
            setInternal(ringName, Iterables.concat(Iterables.transform(ring.entries, input -> input.ringMember), Collections.singleton(ringMember)));
        }
    }

    private void setInternal(byte[] ringName, Iterable<RingMember> members) throws Exception {
        /*
         We deliberately do a slab update of rings to ensure "all at once" ring visibility.
         */
        systemWALStorage.update(PartitionCreator.RING_INDEX, null,
            (highwater, scan) -> {
                long timestampAndVersion = orderIdProvider.nextId();
                for (RingMember member : members) {
                    if (!scan.row(-1, ringStoreReader.key(ringName, member), new byte[0], timestampAndVersion, false, timestampAndVersion)) {
                        return false;
                    }
                }
                return true;
            }, walUpdated);
        ringsCache.remove(ringName);
        //ringsCache.remove(new IBA(ringName));

        LOG.info("Ring update:{} -> {}", new String(ringName), members);
    }

    @Override
    public void removeRingMember(byte[] ringName, RingMember ringMember) throws Exception {
        Preconditions.checkNotNull(ringName, "ringName cannot be null.");
        Preconditions.checkNotNull(ringMember, "ringMember cannot be null.");
        byte[] key = ringStoreReader.key(ringName, ringMember);
        TimestampedValue had = systemWALStorage.getTimestampedValue(PartitionCreator.RING_INDEX, null, key);
        if (had != null) {
            long timestampAndVersion = orderIdProvider.nextId();
            systemWALStorage.update(PartitionCreator.RING_INDEX, null,
                (highwater, scan) -> scan.row(-1, key, null, timestampAndVersion, true, timestampAndVersion),
                walUpdated);
        }
    }

}
