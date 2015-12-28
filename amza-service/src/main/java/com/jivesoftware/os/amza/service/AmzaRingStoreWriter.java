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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.shared.ring.CacheId;
import com.jivesoftware.os.amza.shared.ring.RingSet;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class AmzaRingStoreWriter implements AmzaRingWriter, RowChanges {

    public enum Status {

        online((byte) 2), joining((byte) 1), off((byte) 0), leaving((byte) -1), offline((byte) -2);

        public final byte serializedByte;

        Status(byte b) {
            this.serializedByte = b;
        }

        public byte[] toBytes() {
            return new byte[] { serializedByte };
        }

        static Status fromBytes(byte[] b) {
            for (Status v : values()) {
                if (v.serializedByte == b[0]) {
                    return v;
                }
            }
            return null;
        }
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRingStoreReader ringStoreReader;
    private final SystemWALStorage systemWALStorage;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final WALUpdated walUpdated;
    private final ConcurrentMap<IBA, CacheId<RingTopology>> ringsCache;
    private final ConcurrentMap<RingMember, CacheId<RingSet>> ringMemberRingNamesCache;
    private final AtomicLong nodeCacheId;

    public AmzaRingStoreWriter(AmzaRingStoreReader ringStoreReader,
        SystemWALStorage systemWALStorage,
        TimestampedOrderIdProvider orderIdProvider,
        WALUpdated walUpdated,
        ConcurrentMap<IBA, CacheId<RingTopology>> ringsCache,
        ConcurrentMap<RingMember, CacheId<RingSet>> ringMemberRingNamesCache,
        AtomicLong nodeCacheId) {
        this.ringStoreReader = ringStoreReader;
        this.systemWALStorage = systemWALStorage;
        this.orderIdProvider = orderIdProvider;
        this.walUpdated = walUpdated;
        this.ringsCache = ringsCache;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
        this.nodeCacheId = nodeCacheId;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.RING_INDEX.equals(changes.getVersionedPartitionName())) {
            for (WALKey walKey : changes.getApply().keySet()) {
                byte[] ringBytes = ringStoreReader.keyToRingName(walKey);
                ringsCache.compute(new IBA(ringBytes), (iba, cacheIdRingTopology) -> {
                    if (cacheIdRingTopology == null) {
                        cacheIdRingTopology = new CacheId<>(null);
                    }
                    cacheIdRingTopology.currentCacheId++;
                    /*LOG.info("Rings advanced {} to {}", Arrays.toString(ringBytes), cacheIdRingTopology.currentCacheId);*/
                    return cacheIdRingTopology;
                });
                RingMember ringMember = ringStoreReader.keyToRingMember(walKey.key);
                ringMemberRingNamesCache.compute(ringMember, (ringMember1, cacheIdRingSet) -> {
                    if (cacheIdRingSet == null) {
                        cacheIdRingSet = new CacheId<>(null);
                    }
                    cacheIdRingSet.currentCacheId++;
                    /*LOG.info("Members advanced {} to {}", ringMember, cacheIdRingSet.currentCacheId);*/
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
        RingTopology ring = ringStoreReader.getRing(AmzaRingReader.SYSTEM_RING);
        if (ring.entries.size() < desiredRingSize) {
            throw new IllegalStateException("Current 'system' ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        List<RingMember> ringAsList = Lists.newArrayList(Lists.transform(ring.entries, input -> input.ringMember));
        Collections.sort(ringAsList, (o1, o2) -> Integer.compare(new HashCodeBuilder().append(o1).append(ringName).toHashCode(),
            new HashCodeBuilder().append(o2).append(ringName).toHashCode()));

        RingTopology existingRing = ringStoreReader.getRing(ringName);
        Set<RingMember> orderedRing = new HashSet<>();
        orderedRing.addAll(Lists.transform(existingRing.entries, input -> input.ringMember));

        Iterator<RingMember> ringIter = ringAsList.iterator();
        while (orderedRing.size() < desiredRingSize && ringIter.hasNext()) {
            orderedRing.add(ringIter.next());
        }

        setInternal(ringName, orderedRing);
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
        ringsCache.remove(new IBA(ringName));

        if (LOG.isInfoEnabled()) {
            LOG.info("Ring update:{} -> {}", new String(ringName), members);
        }
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
