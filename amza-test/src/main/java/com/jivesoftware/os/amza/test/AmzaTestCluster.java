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
package com.jivesoftware.os.amza.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaChangeIdPacker;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.RegionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.HighwaterStorage;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class AmzaTestCluster {

    private static final TimestampedOrderIdProvider ORDER_ID_PROVIDER = new OrderIdProviderImpl(new ConstantWriterIdProvider(1),
        new AmzaChangeIdPacker(), new JiveEpochTimestampProvider());

    private final File workingDirctory;
    private final ConcurrentSkipListMap<RingMember, AmzaNode> cluster = new ConcurrentSkipListMap<>();
    private int oddsOfAConnectionFailureWhenAdding = 0; // 0 never - 100 always
    private int oddsOfAConnectionFailureWhenTaking = 0; // 0 never - 100 always
    private AmzaService lastAmzaService = null;

    public AmzaTestCluster(File workingDirctory,
        int oddsOfAConnectionFailureWhenAdding,
        int oddsOfAConnectionFailureWhenTaking) {
        this.workingDirctory = workingDirctory;
        this.oddsOfAConnectionFailureWhenAdding = oddsOfAConnectionFailureWhenAdding;
        this.oddsOfAConnectionFailureWhenTaking = oddsOfAConnectionFailureWhenTaking;
    }

    public Collection<AmzaNode> getAllNodes() {
        return cluster.values();
    }

    public AmzaNode get(RingMember ringMember) {
        return cluster.get(ringMember);
    }

    public void remove(RingMember ringMember) {
        cluster.remove(ringMember);
    }

    public AmzaNode newNode(final RingMember ringMember, final RingHost ringHost) throws Exception {

        AmzaNode service = cluster.get(ringMember);
        if (service != null) {
            return service;
        }

        AmzaServiceConfig config = new AmzaServiceConfig();
        config.workingDirectories = new String[]{workingDirctory.getAbsolutePath() + "/" + ringHost.getHost() + "-" + ringHost.getPort()};
        config.resendReplicasIntervalInMillis = 100;
        config.applyReplicasIntervalInMillis = 100;
        config.takeFromNeighborsIntervalInMillis = 1000;
        config.compactTombstoneIfOlderThanNMillis = 100000L;

        UpdatesSender changeSetSender = (Entry<RingMember, RingHost> node, RegionName mapName, Commitable<WALValue> changes) -> {
            AmzaNode service1 = cluster.get(node.getKey());
            if (service1 == null) {
                throw new IllegalStateException("Service doesn't exists for " + node.getValue());
            } else {
                service1.addToReplicatedWAL(mapName, changes);
            }
        };

        UpdatesTaker taker = (Entry<RingMember, RingHost> node, RegionName regionName, long transactionId, RowStream tookRowUpdates) -> {
            AmzaNode service1 = cluster.get(node.getKey());
            if (service1 == null) {
                throw new IllegalStateException("Service doesn't exists for " + node.getValue());
            } else {
                service1.takeRegion(regionName, transactionId, tookRowUpdates);
            }
            return new HashMap<>();
        };

        // TODO need to get writer id from somewhere other than port.
        final TimestampedOrderIdProvider orderIdProvider = ORDER_ID_PROVIDER;

        final ObjectMapper mapper = new ObjectMapper();
        RegionPropertyMarshaller regionPropertyMarshaller = new RegionPropertyMarshaller() {

            @Override
            public RegionProperties fromBytes(byte[] bytes) throws Exception {
                return mapper.readValue(bytes, RegionProperties.class);
            }

            @Override
            public byte[] toBytes(RegionProperties regionProperties) throws Exception {
                return mapper.writeValueAsBytes(regionProperties);
            }
        };

        AmzaStats amzaStats = new AmzaStats();
        HighwaterStorage highWaterMarks = new MemoryBackedHighwaterStorage();

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(config,
            amzaStats,
            ringMember,
            ringHost,
            orderIdProvider,
            regionPropertyMarshaller,
            new WALIndexProviderRegistry(),
            changeSetSender,
            taker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(), (RowsChanged changes) -> {
            });

        amzaService.start();

        final RegionName regionName = new RegionName(false, "test", "region1");
        amzaService.watch(regionName, (RowsChanged changes) -> {
            if (changes.getApply().size() > 0) {
                System.out.println("Service:" + ringMember
                    + " Region:" + regionName.getRegionName()
                    + " Changed:" + changes.getApply().size());
            }
        });

        try {
            amzaService.getAmzaRing().addRingMember("system", ringMember); // ?? Hacky
            amzaService.getAmzaRing().addRingMember("test", ringMember); // ?? Hacky
            if (lastAmzaService != null) {
                amzaService.getAmzaRing().register(lastAmzaService.getAmzaRing().getRingMember(), lastAmzaService.getAmzaRing().getRingHost());
                amzaService.getAmzaRing().addRingMember("system", lastAmzaService.getAmzaRing().getRingMember()); // ?? Hacky
                amzaService.getAmzaRing().addRingMember("test", lastAmzaService.getAmzaRing().getRingMember()); // ?? Hacky

                lastAmzaService.getAmzaRing().register(ringMember, ringHost);
                lastAmzaService.getAmzaRing().addRingMember("system", ringMember); // ?? Hacky
                lastAmzaService.getAmzaRing().addRingMember("test", ringMember); // ?? Hacky
            }
            lastAmzaService = amzaService;
        } catch (Exception x) {
            x.printStackTrace();
            System.out.println("FAILED CONNECTING RING");
            System.exit(1);
        }

        service = new AmzaNode(ringHost, amzaService, highWaterMarks);
        cluster.put(ringMember, service);
        System.out.println("Added serviceHost:" + ringMember + " to the cluster.");
        return service;
    }

    public class AmzaNode {

        private final Random random = new Random();
        private final RingHost serviceHost;
        private final AmzaService amzaService;
        private final HighwaterStorage highWaterMarks;
        private boolean off = false;
        private int flapped = 0;

        public AmzaNode(RingHost serviceHost, AmzaService amzaService, HighwaterStorage highWaterMarks) {
            this.serviceHost = serviceHost;
            this.amzaService = amzaService;
            this.highWaterMarks = highWaterMarks;
        }

        @Override
        public String toString() {
            return serviceHost.toString();
        }

        public boolean isOff() {
            return off;
        }

        public void setOff(boolean off) {
            this.off = off;
            flapped++;
        }

        public void stop() throws Exception {
            amzaService.stop();
        }

        public void create(RegionName regionName) throws Exception {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
            amzaService.createRegionIfAbsent(regionName, new RegionProperties(storageDescriptor, 2, 2, false));
        }

        void addToReplicatedWAL(RegionName mapName, Commitable<WALValue> changes) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenAdding)) {
                throw new RuntimeException("Random connection failure:" + serviceHost);
            }
            amzaService.updates(mapName, changes);
        }

        public void update(RegionName regionName, WALKey k, byte[] v, long timestamp, boolean tombstone) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            AmzaRegion amzaRegion = amzaService.getRegion(regionName);
            if (tombstone) {
                amzaRegion.remove(k);
            } else {
                amzaRegion.set(k, v);
            }

        }

        public byte[] get(RegionName regionName, WALKey key) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }

            AmzaRegion amzaRegion = amzaService.getRegion(regionName);
            return amzaRegion.get(key);
        }

        public void takeRegion(RegionName regionName, long transationId, RowStream rowStream) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenTaking)) {
                throw new RuntimeException("Random take failure:" + serviceHost);
            }
            AmzaRegion got = amzaService.getRegion(regionName);
            if (got != null) {
                got.takeRowUpdatesSince(transationId, rowStream);
            } else {
                throw new RuntimeException("Failed to take because region:" + regionName + " is missing");
            }
        }

        public void printService() throws Exception {
            if (off) {
                System.out.println(serviceHost.getHost() + ":" + serviceHost.getPort() + " is OFF flapped:" + flapped);
            }
        }

        public boolean compare(AmzaNode service) throws Exception {
            if (off || service.off) {
                return true;
            }

            Set<RegionName> allARegions = amzaService.getRegionNames();
            Set<RegionName> allBRegions = service.amzaService.getRegionNames();

            if (allARegions.size() != allBRegions.size()) {
                System.out.println(allARegions + " -vs- " + allBRegions);
                return false;
            }

            Set<RegionName> regionNames = new HashSet<>();
            regionNames.addAll(allARegions);
            regionNames.addAll(allBRegions);

            NavigableMap<RingMember, RingHost> aRing = amzaService.getAmzaRingReader().getRing("system");
            NavigableMap<RingMember, RingHost> bRing = service.amzaService.getAmzaRingReader().getRing("system");

            if (!aRing.equals(bRing)) {
                System.out.println(aRing + "-vs-" + bRing);
                return false;
            }

            for (RegionName regionName : regionNames) {
                if (regionName.equals(RegionProvider.HIGHWATER_MARK_INDEX)) {
                    continue;
                }

                AmzaRegion a = amzaService.getRegion(regionName);
                AmzaRegion b = service.amzaService.getRegion(regionName);
                if (a == null || b == null) {
                    System.out.println(regionName + " " + amzaService.getAmzaRing().getRingMember() + " " + a + " -- vs --"
                        + service.amzaService.getAmzaRing().getRingMember() + " " + b);
                    return false;
                }
                if (!a.compare(b)) {
                    System.out.println(highWaterMarks + " -vs- " + service.highWaterMarks);
                    return false;
                }
            }
            return true;
        }

    }
}
