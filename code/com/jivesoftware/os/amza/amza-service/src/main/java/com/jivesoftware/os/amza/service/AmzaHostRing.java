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

import com.jivesoftware.os.amza.service.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.HostRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALStorageUpateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class AmzaHostRing implements AmzaRing {

    public static enum Status {

        online((byte) 2), joining((byte) 1), off((byte) 0), leaving((byte) -1), offline((byte) -2);

        public final byte b;

        Status(byte b) {
            this.b = b;
        }

        public byte[] toBytes() {
            return new byte[]{b};
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
    private final RingHost ringHost;
    private final RegionProvider regionProvider;
    private final TimestampedOrderIdProvider orderIdProvider;

    public AmzaHostRing(RingHost ringHost,
        RegionProvider regionProvider,
        TimestampedOrderIdProvider orderIdProvider) {
        this.ringHost = ringHost;
        this.regionProvider = regionProvider;
        this.orderIdProvider = orderIdProvider;
    }

    private WALKey key(String ringName, RingHost ringHost) throws IOException {
        MemoryFiler filer = new MemoryFiler();
        UIO.writeString(filer, ringName.toUpperCase(), "ringName");
        UIO.writeByte(filer, 0, "seperator");
        if (ringHost != null) {
            UIO.writeByteArray(filer, ringHost.toBytes(), "ringHost");
        }
        return new WALKey(filer.getBytes());
    }

    public RingHost keyToRingHost(WALKey key) throws IOException {
        MemoryFiler filer = new MemoryFiler(key.getKey());
        UIO.readString(filer, "ringName");
        UIO.readByte(filer, "seperator");
        return RingHost.fromBytes(UIO.readByteArray(filer, "ringHost"));
    }

    public RingHost getRingHost() {
        return ringHost;
    }

    public void buildRandomSubRing(String ringName, int desiredRingSize) throws Exception {
        List<RingHost> ring = getRing("system");
        if (ring.size() < desiredRingSize) {
            throw new IllegalStateException("Current 'system' ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        Collections.shuffle(ring, new Random(ringName.hashCode()));
        for (int i = 0; i < desiredRingSize; i++) {
            addRingHost(ringName, ring.get(i));
        }
    }

    @Override
    public HostRing getHostRing(String ringName) throws Exception {
        return new HostRingBuilder().build(ringHost, getRing(ringName));
    }

    @Override
    public List<RingHost> getRing(String ringName) throws Exception {
        RegionStore ringIndex = regionProvider.getRingIndexStore();

        final Set<RingHost> ringHosts = new HashSet<>();
        WALKey from = key(ringName, null);
        ringIndex.rangeScan(from, from.prefixUpperExclusive(), new WALScan() {
            @Override
            public boolean row(long orderId, WALKey key, WALValue value) throws Exception {
                ringHosts.add(keyToRingHost(key));
                return true;
            }
        });
        if (ringHosts.isEmpty()) {
            //LOG.warn("No ring defined for ringName:" + ringName);
            return new ArrayList<>();
        } else {
            return new ArrayList<>(ringHosts);
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
        RegionStore ringIndex = regionProvider.getRingIndexStore();
        WALKey key = key(ringName, ringHost);
        WALValue had = ringIndex.get(key);
        if (had == null) {
            RowStoreUpdates tx = ringIndex.startTransaction(orderIdProvider.nextId());
            tx.add(key, new byte[Status.online.b]);
            tx.commit();
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
        RegionStore ringIndex = regionProvider.getRingIndexStore();
        WALKey key = key(ringName, ringHost);
        WALValue had = ringIndex.get(key);
        if (had != null) {
            RowStoreUpdates tx = ringIndex.startTransaction(orderIdProvider.nextId());
            tx.remove(key);
            tx.commit(WALStorageUpateMode.updateThenReplicate);
        }
    }

    @Override
    public void allRings(final RingStream ringStream) throws Exception {
        regionProvider.getRingIndexStore().rowScan(new WALScan() {

            @Override
            public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                MemoryFiler filer = new MemoryFiler(key.getKey());
                String ringName = UIO.readString(filer, "ringName");
                UIO.readByte(filer, "seperator");
                return ringStream.stream(ringName, Status.fromBytes(value.getValue()).name(), RingHost.fromBytes(UIO.readByteArray(filer, "ringHost")));
            }
        });
    }
}
