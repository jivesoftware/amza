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

import com.jivesoftware.os.amza.service.replication.HostRing;
import com.jivesoftware.os.amza.service.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.RowStoreUpdates;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.Marshaller;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AmzaHostRing implements AmzaRing {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingHost ringHost;
    private final RegionProvider ringStoreProvider;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final Marshaller marshaller;

    public AmzaHostRing(RingHost ringHost,
        RegionProvider ringStoreProvider,
        TimestampedOrderIdProvider orderIdProvider,
        Marshaller marshaller) {
        this.ringHost = ringHost;
        this.ringStoreProvider = ringStoreProvider;
        this.orderIdProvider = orderIdProvider;
        this.marshaller = marshaller;
    }

    public RingHost getRingHost() {
        return ringHost;
    }

    public void buildRandomSubRing(String ringName, int desiredRingSize) throws Exception {
        List<RingHost> ring = getRing("MASTER");
        if (ring.size() < desiredRingSize) {
            throw new IllegalStateException("Current master ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        Collections.shuffle(ring);
        for (int i = 0; i < desiredRingSize; i++) {
            addRingHost(ringName, ring.get(i));
        }
    }

    public HostRing getHostRing(String ringName) throws Exception {
        return new HostRingBuilder().build(ringHost, getRing(ringName));
    }

    @Override
    public List<RingHost> getRing(String ringName) throws Exception {
        RegionName ringIndexKey = createRingName(ringName);
        RegionStore ringIndex = ringStoreProvider.get(ringIndexKey);
        if (ringIndex == null) {
            LOG.warn("No ring defined for ringName:" + ringName);
            return new ArrayList<>();
        } else {
            final Set<RingHost> ringHosts = new HashSet<>();
            ringIndex.rowScan(new WALScan() {
                @Override
                public boolean row(long orderId, WALKey key, WALValue value) throws Exception {
                    if (!value.getTombstoned()) {
                        ringHosts.add(marshaller.deserialize(value.getValue(), RingHost.class));
                    }
                    return true;
                }
            });
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
        byte[] rawRingHost = marshaller.serialize(ringHost);
        RegionName ringIndexKey = createRingName(ringName);
        RegionStore ringIndex = ringStoreProvider.get(ringIndexKey);
        RowStoreUpdates tx = ringIndex.startTransaction(orderIdProvider.nextId());
        tx.add(new WALKey(rawRingHost), rawRingHost);
        tx.commit();
    }

    @Override
    public void removeRingHost(String ringName, RingHost ringHost) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        if (ringHost == null) {
            throw new IllegalArgumentException("ringHost cannot be null.");
        }
        byte[] rawRingHost = marshaller.serialize(ringHost);
        RegionName ringIndexKey = createRingName(ringName);
        RegionStore ringIndex = ringStoreProvider.get(ringIndexKey);
        RowStoreUpdates tx = ringIndex.startTransaction(orderIdProvider.nextId());
        tx.remove(new WALKey(rawRingHost));
        tx.commit();
    }

    private RegionName createRingName(String ringName) {
        ringName = ringName.toUpperCase();
        return new RegionName("MASTER", "RING_INDEX_" + ringName, null, null);
    }
}
