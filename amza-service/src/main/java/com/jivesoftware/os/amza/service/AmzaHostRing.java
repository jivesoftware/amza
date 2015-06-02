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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.replication.RegionStripe;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.RingNeighbors;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

public class AmzaHostRing implements AmzaRing, RowChanges {

    public static enum Status {

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
    private final AmzaRingReader ringReader;
    private final RegionStripe systemRegionStripe;
    private final WALReplicator replicator;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final ConcurrentMap<String, Integer> ringSizes = Maps.newConcurrentMap();

    public AmzaHostRing(AmzaRingReader ringReader,
        RegionStripe systemRegionStripe,
        WALReplicator replicator,
        TimestampedOrderIdProvider orderIdProvider) {
        this.ringReader = ringReader;
        this.systemRegionStripe = systemRegionStripe;
        this.replicator = replicator;
        this.orderIdProvider = orderIdProvider;
    }

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (RegionProvider.RING_INDEX.equals(changes.getVersionedRegionName())) {
            for (WALKey key : changes.getApply().columnKeySet()) {
                ringSizes.remove(ringReader.keyToRingName(key));
            }
        }
    }

    @Override
    public void register(RingMember ringMember, RingHost ringHost) throws Exception {
        WALValue registeredHost = systemRegionStripe.get(RegionProvider.NODE_INDEX.getRegionName(), new WALKey(ringMember.toBytes()));
        if (registeredHost != null && ringHost.equals(RingHost.fromBytes(registeredHost.getValue()))) {
            return;
        }
        systemRegionStripe.commit(RegionProvider.NODE_INDEX.getRegionName(),
            Optional.absent(),
            false,
            replicator,
            WALStorageUpdateMode.noReplication,
            (highwater, scan) -> {
                scan.row(-1, new WALKey(ringMember.toBytes()), new WALValue(ringHost.toBytes(), orderIdProvider.nextId(), false));
            });
        LOG.info("register ringMember:{} as ringHost:{}", ringMember, ringHost);
    }

    @Override
    public void deregister(RingMember ringMember) throws Exception {
        systemRegionStripe.commit(RegionProvider.NODE_INDEX.getRegionName(),
            Optional.absent(),
            false,
            replicator,
            WALStorageUpdateMode.noReplication,
            (highwater, scan) -> {
                scan.row(-1, new WALKey(ringMember.toBytes()), new WALValue(null, orderIdProvider.nextId(), true));
            });
        LOG.info("deregister ringMember:{}");
    }

    public RingMember getRingMember() {
        return ringReader.getRingMember();
    }

    public RingHost getRingHost() throws Exception {
        WALValue registeredHost = systemRegionStripe.get(RegionProvider.NODE_INDEX.getRegionName(), new WALKey(getRingMember().toBytes()));
        if (registeredHost != null) {
            return RingHost.fromBytes(registeredHost.getValue());
        } else {
            return RingHost.UNKNOWN_RING_HOST;
        }
    }

    public boolean isMemberOfRing(String ringName) throws Exception {
        return ringReader.isMemberOfRing(ringName);
    }

    @Override
    public RingNeighbors getRingNeighbors(String ringName) throws Exception {
        return ringReader.getRingNeighbors(ringName);
    }

    @Override
    public NavigableMap<RingMember, RingHost> getRing(String ringName) throws Exception {
        return ringReader.getRing(ringName);
    }

    @Override
    public int getRingSize(String ringName) throws Exception {
        return ringSizes.computeIfAbsent(ringName, key -> {
            try {
                return ringReader.getRing(key).size();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void ensureMaximalSubRing(String ringName) throws Exception {
        ensureSubRing(ringName, getRingSize("system"));
    }

    public void ensureSubRing(String ringName, int desiredRingSize) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        int ringSize = getRingSize(ringName);
        if (ringSize < desiredRingSize) {
            LOG.info("Ring {} will grow, has {} desires {}", ringName, ringSize, desiredRingSize);
            buildRandomSubRing(ringName, desiredRingSize);
        }
    }

    public void buildRandomSubRing(String ringName, int desiredRingSize) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        NavigableMap<RingMember, RingHost> ring = ringReader.getRing("system");
        if (ring.size() < desiredRingSize) {
            throw new IllegalStateException("Current 'system' ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        List<RingMember> ringAsList = new ArrayList<>(ring.keySet());
        Collections.sort(ringAsList, (RingMember o1, RingMember o2) -> {
            return Integer.compare(Objects.hash(o1, ringName), Objects.hash(o2, ringName));
        });

        NavigableMap<RingMember, RingHost> existingRing = ringReader.getRing(ringName);
        if (existingRing == null) {
            existingRing = new TreeMap<>();
        }
        setInternal(ringName, Iterables.concat(existingRing.keySet(), Iterables.limit(ringAsList, desiredRingSize)));
    }

    @Override
    public void addRingMember(String ringName, RingMember ringMember) throws Exception {
        Preconditions.checkNotNull(ringName, "ringName cannot be null.");
        Preconditions.checkNotNull(ringMember, "ringMember cannot be null.");
        final WALKey key = ringReader.key(ringName, ringMember);
        WALValue had = systemRegionStripe.get(RegionProvider.RING_INDEX.getRegionName(), key);
        if (had == null || had.getTombstoned()) {
            NavigableMap<RingMember, RingHost> ring = ringReader.getRing(ringName);
            setInternal(ringName, Iterables.concat(ring.keySet(), Collections.singleton(ringMember)));
        }
    }

    private void setInternal(String ringName, Iterable<RingMember> members) throws Exception {
        /*
         We deliberatly do a slab update of rings to ensure "all at once" ring visibility.
         */

        systemRegionStripe.commit(RegionProvider.RING_INDEX.getRegionName(),
            Optional.absent(),
            false,
            replicator,
            WALStorageUpdateMode.replicateThenUpdate,
            (highwater, scan) -> {
                long timestamp = orderIdProvider.nextId();
                for (RingMember member : members) {
                    scan.row(-1, ringReader.key(ringName, member), new WALValue(new byte[0], timestamp, false));
                }
            });

        LOG.info("Ring update:{} -> {}", ringName, members);
    }

    @Override
    public void removeRingMember(String ringName, RingMember ringMember) throws Exception {
        Preconditions.checkNotNull(ringName, "ringName cannot be null.");
        Preconditions.checkNotNull(ringMember, "ringMember cannot be null.");
        final WALKey key = ringReader.key(ringName, ringMember);
        WALValue had = systemRegionStripe.get(RegionProvider.RING_INDEX.getRegionName(), key);
        if (had != null) {
            systemRegionStripe.commit(RegionProvider.RING_INDEX.getRegionName(),
                Optional.absent(),
                false,
                replicator,
                WALStorageUpdateMode.replicateThenUpdate,
                (highwater, scan) -> {
                    scan.row(-1, key, new WALValue(null, orderIdProvider.nextId(), true));
                });
        }
    }

    /**
     @param ringStream
     @throws Exception
     */
    @Override
    public void allRings(final RingStream ringStream) throws Exception {
        Map<RingMember, RingHost> ringMemberToRingHost = new HashMap<>();
        systemRegionStripe.rowScan(RegionProvider.NODE_INDEX.getRegionName(), (long rowTxId, WALKey key, WALValue rawRingHost) -> {
            RingMember ringMember = RingMember.fromBytes(key.getKey());
            RingHost ringHost = RingHost.fromBytes(rawRingHost.getValue());
            ringMemberToRingHost.put(ringMember, ringHost);
            return true;
        });

        systemRegionStripe.rowScan(RegionProvider.RING_INDEX.getRegionName(), (long rowTxId, WALKey key, WALValue value) -> {
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
