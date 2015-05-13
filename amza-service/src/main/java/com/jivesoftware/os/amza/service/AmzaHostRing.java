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

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.replication.RegionStripe;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.HostRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class AmzaHostRing implements AmzaRing, RowChanges {

    public static enum Status {

        online((byte) 2), joining((byte) 1), off((byte) 0), leaving((byte) -1), offline((byte) -2);

        public final byte b;

        Status(byte b) {
            this.b = b;
        }

        public byte[] toBytes() {
            return new byte[] { b };
        }

        static Status fromBytes(byte[] b) {
            for (Status v : values()) {
                if (v.b == b[0]) {
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
        if (RegionProvider.RING_INDEX.equals(changes.getRegionName())) {
            for (WALKey key : changes.getApply().columnKeySet()) {
                ringSizes.remove(ringReader.keyToRingName(key));
            }
        }
    }

    public RingHost getRingHost() {
        return ringReader.getRingHost();
    }

    @Override
    public HostRing getHostRing(String ringName) throws Exception {
        return ringReader.getHostRing(ringName);
    }

    @Override
    public List<RingHost> getRing(String ringName) throws Exception {
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
        List<RingHost> ring = ringReader.getRing("system");
        if (ring.size() < desiredRingSize) {
            throw new IllegalStateException("Current 'system' ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        Collections.shuffle(ring, new Random(ringName.hashCode()));
        for (int i = 0; i < desiredRingSize; i++) {
            addInternal(ringName, ring.get(i));
        }
    }

    @Override
    public void addRingHost(String ringName, RingHost ringHost) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        if (ringHost == null) {
            throw new IllegalArgumentException("ringHost cannot be null.");
        }
        addInternal(ringName, ringHost);
    }

    private void addInternal(String ringName, RingHost ringHost) throws Exception {
        final WALKey key = ringReader.key(ringName, ringHost);
        WALValue had = systemRegionStripe.get(RegionProvider.RING_INDEX, key);
        if (had == null) {
            systemRegionStripe.commit(RegionProvider.RING_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (Scan<WALValue> scan) -> {
                scan.row(-1, key, new WALValue(new byte[Status.online.b], orderIdProvider.nextId(), false));
            });
        }
    }

    @Override
    public void removeRingHost(String ringName, RingHost ringHost) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        if (ringHost == null) {
            throw new IllegalArgumentException("ringHost cannot be null.");
        }
        final WALKey key = ringReader.key(ringName, ringHost);
        WALValue had = systemRegionStripe.get(RegionProvider.RING_INDEX, key);
        if (had != null) {
            systemRegionStripe.commit(RegionProvider.RING_INDEX, replicator, WALStorageUpdateMode.replicateThenUpdate, (Scan<WALValue> scan) -> {
                scan.row(-1, key, new WALValue(null, orderIdProvider.nextId(), true));
            });
        }
    }

    @Override
    public void allRings(final RingStream ringStream) throws Exception {
        systemRegionStripe.rowScan(RegionProvider.RING_INDEX, (long rowTxId, WALKey key, WALValue value) -> {
            MemoryFiler filer = new MemoryFiler(key.getKey());
            String ringName = UIO.readString(filer, "ringName");
            UIO.readByte(filer, "seperator");
            return ringStream.stream(ringName, Status.fromBytes(value.getValue()).name(), RingHost.fromBytes(UIO.readByteArray(filer, "ringHost")));
        });
    }
}
