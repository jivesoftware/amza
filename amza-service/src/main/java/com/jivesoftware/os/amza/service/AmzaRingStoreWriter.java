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
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class AmzaRingStoreWriter implements AmzaRingWriter, RowChanges {

    public enum Status {

        online((byte) 2), joining((byte) 1), off((byte) 0), leaving((byte) -1), offline((byte) -2);

        public final byte serializedByte;

        Status(byte b) {
            this.serializedByte = b;
        }

        public byte[] toBytes() {
            return new byte[]{serializedByte};
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
    private final ConcurrentMap<String, Integer> ringSizes;
    private final ConcurrentMap<RingMember, Set<String>> ringMemberRingNamesCache;
    private final WALUpdated walUpdated;

    public AmzaRingStoreWriter(AmzaRingStoreReader ringStoreReader,
        SystemWALStorage systemWALStorage,
        TimestampedOrderIdProvider orderIdProvider,
        WALUpdated walUpdated,
        ConcurrentMap<String, Integer> ringSizes,
        ConcurrentMap<RingMember, Set<String>> ringMemberRingNamesCache) {
        this.ringStoreReader = ringStoreReader;
        this.systemWALStorage = systemWALStorage;
        this.orderIdProvider = orderIdProvider;
        this.walUpdated = walUpdated;
        this.ringSizes = ringSizes;
        this.ringMemberRingNamesCache = ringMemberRingNamesCache;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionProvider.RING_INDEX.equals(changes.getVersionedPartitionName())) {
            for (WALKey key : changes.getApply().columnKeySet()) {
                ringSizes.remove(ringStoreReader.keyToRingName(key));
                ringMemberRingNamesCache.remove(ringStoreReader.keyToRingMember(key.getKey()));
            }
        }
    }

    @Override
    public void register(RingMember ringMember, RingHost ringHost) throws Exception {
        TimestampedValue registeredHost = systemWALStorage.get(PartitionProvider.NODE_INDEX, ringMember.toBytes());
        if (registeredHost != null && ringHost.equals(RingHost.fromBytes(registeredHost.getValue()))) {
            return;
        }
        systemWALStorage.update(PartitionProvider.NODE_INDEX,
            (highwater, scan) -> {
                return scan.row(-1, ringMember.toBytes(), ringHost.toBytes(), orderIdProvider.nextId(), false);
            }, walUpdated);
        LOG.info("register ringMember:{} as ringHost:{}", ringMember, ringHost);
    }

    @Override
    public void deregister(RingMember ringMember) throws Exception {
        systemWALStorage.update(PartitionProvider.NODE_INDEX,
            (highwater, scan) -> {
                return scan.row(-1, ringMember.toBytes(), null, orderIdProvider.nextId(), true);
            }, walUpdated);
        LOG.info("deregister ringMember:{}");
    }

    public RingHost getRingHost() throws Exception {
        TimestampedValue registeredHost = systemWALStorage.get(PartitionProvider.NODE_INDEX, ringStoreReader.getRingMember().toBytes());
        if (registeredHost != null) {
            return RingHost.fromBytes(registeredHost.getValue());
        } else {
            return RingHost.UNKNOWN_RING_HOST;
        }
    }

    public boolean isMemberOfRing(String ringName) throws Exception {
        return ringStoreReader.isMemberOfRing(ringName);
    }

    public void ensureMaximalSubRing(String ringName) throws Exception {
        ensureSubRing(ringName, ringStoreReader.getRingSize(AmzaRingReader.SYSTEM_RING));
    }

    public void ensureSubRing(String ringName, int desiredRingSize) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        int ringSize = ringStoreReader.getRingSize(ringName);
        if (ringSize < desiredRingSize) {
            LOG.info("Ring {} will grow, has {} desires {}", ringName, ringSize, desiredRingSize);
            buildRandomSubRing(ringName, desiredRingSize);
        }
    }

    public void buildRandomSubRing(String ringName, int desiredRingSize) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        NavigableMap<RingMember, RingHost> ring = ringStoreReader.getRing(AmzaRingReader.SYSTEM_RING);
        if (ring.size() < desiredRingSize) {
            throw new IllegalStateException("Current 'system' ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        List<RingMember> ringAsList = new ArrayList<>(ring.keySet());
        Collections.sort(ringAsList, (RingMember o1, RingMember o2) -> {
            return Integer.compare(Objects.hash(o1, ringName), Objects.hash(o2, ringName));
        });

        NavigableMap<RingMember, RingHost> existingRing = ringStoreReader.getRing(ringName);
        setInternal(ringName, Iterables.concat(existingRing.keySet(), Iterables.limit(ringAsList, desiredRingSize)));
    }

    @Override
    public void addRingMember(String ringName, RingMember ringMember) throws Exception {
        Preconditions.checkNotNull(ringName, "ringName cannot be null.");
        Preconditions.checkNotNull(ringMember, "ringMember cannot be null.");
        byte[] key = ringStoreReader.key(ringName, ringMember);
        TimestampedValue had = systemWALStorage.get(PartitionProvider.RING_INDEX, key);
        if (had == null) {
            NavigableMap<RingMember, RingHost> ring = ringStoreReader.getRing(ringName);
            setInternal(ringName, Iterables.concat(ring.keySet(), Collections.singleton(ringMember)));
        }
    }

    private void setInternal(String ringName, Iterable<RingMember> members) throws Exception {
        /*
         We deliberatly do a slab update of rings to ensure "all at once" ring visibility.
         */

        systemWALStorage.update(PartitionProvider.RING_INDEX,
            (highwater, scan) -> {
                long timestamp = orderIdProvider.nextId();
                for (RingMember member : members) {
                    if (!scan.row(-1, ringStoreReader.key(ringName, member), new byte[0], timestamp, false)) {
                        return false;
                    }
                }
                return true;
            }, walUpdated);
        ringSizes.remove(ringName);

        LOG.info("Ring update:{} -> {}", ringName, members);
    }

    @Override
    public void removeRingMember(String ringName, RingMember ringMember) throws Exception {
        Preconditions.checkNotNull(ringName, "ringName cannot be null.");
        Preconditions.checkNotNull(ringMember, "ringMember cannot be null.");
        byte[] key = ringStoreReader.key(ringName, ringMember);
        TimestampedValue had = systemWALStorage.get(PartitionProvider.RING_INDEX, key);
        if (had != null) {
            systemWALStorage.update(PartitionProvider.RING_INDEX,
                (highwater, scan) -> {
                    return scan.row(-1, key, null, orderIdProvider.nextId(), true);
                }, walUpdated);
        }
    }

}
