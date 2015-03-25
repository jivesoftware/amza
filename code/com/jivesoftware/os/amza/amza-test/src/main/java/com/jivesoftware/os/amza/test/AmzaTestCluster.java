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

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaChangeIdPacker;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighWaterMarks;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.MemoryWALIndex;
import com.jivesoftware.os.amza.shared.NoOpWALIndex;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALIndexProvider;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.amza.storage.IndexedWAL;
import com.jivesoftware.os.amza.storage.NonIndexWAL;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowIOProvider;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class AmzaTestCluster {

    private final File workingDirctory;
    private final ConcurrentSkipListMap<RingHost, AmzaNode> cluster = new ConcurrentSkipListMap<>();
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

    public AmzaNode get(RingHost host) {
        return cluster.get(host);
    }

    public void remove(RingHost host) {
        cluster.remove(host);
    }

    public AmzaNode newNode(final RingHost serviceHost) throws Exception {

        AmzaNode service = cluster.get(serviceHost);
        if (service != null) {
            return service;
        }

        AmzaServiceConfig config = new AmzaServiceConfig();
        config.workingDirectories = new String[] { workingDirctory.getAbsolutePath() + "/" + serviceHost.getHost() + "-" + serviceHost.getPort() };
        config.replicationFactor = 2;
        config.takeFromFactor = 2;
        config.resendReplicasIntervalInMillis = 100;
        config.applyReplicasIntervalInMillis = 100;
        config.takeFromNeighborsIntervalInMillis = 1000;
        config.compactTombstoneIfOlderThanNMillis = 100000L;

        UpdatesSender changeSetSender = new UpdatesSender() {
            @Override
            public void sendUpdates(RingHost ringHost,
                RegionName mapName, WALScanable changes) throws Exception {
                AmzaNode service = cluster.get(ringHost);
                if (service == null) {
                    throw new IllegalStateException("Service doesn't exists for " + ringHost);
                } else {
                    service.addToReplicatedWAL(mapName, changes);
                }
            }
        };

        UpdatesTaker taker = new UpdatesTaker() {

            @Override
            public void takeUpdates(RingHost ringHost,
                RegionName regionName,
                long transationId,
                WALScan rowStream) throws Exception {
                AmzaNode service = cluster.get(ringHost);
                if (service == null) {
                    throw new IllegalStateException("Service doesn't exists for " + ringHost);
                } else {
                    service.takeRegion(regionName, transationId, rowStream);
                }
            }
        };

        // TODO need to get writer id from somewhere other than port.
        final TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(serviceHost.getPort()),
            new AmzaChangeIdPacker(), new JiveEpochTimestampProvider());

        final WALIndexProvider walIndexProvider = new WALIndexProvider() {

            @Override
            public WALIndex createIndex(RegionName regionName) throws Exception {

                return new MemoryWALIndex();
            }
        };

        WALStorageProvider walStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory, String domain, RegionName regionName, WALReplicator walReplicator) throws Exception {
                File dir = new File(workingDirectory, domain + File.separator);
                dir.getParentFile().mkdirs();
                RowMarshaller<byte[]> rowMarshaller = new BinaryRowMarshaller();
                RowIOProvider rowIOProvider = new BinaryRowIOProvider();
                return new IndexedWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryWALTx(dir,
                        regionName.getRegionName() + ".kvt",
                        rowIOProvider,
                        rowMarshaller,
                        walIndexProvider,
                        100), walReplicator, 1000);
            }
        };

        final WALIndexProvider replicateAndResendWALIndexProvider = new WALIndexProvider() {

            @Override
            public WALIndex createIndex(RegionName regionName) throws Exception {

                return new NoOpWALIndex();
            }
        };

        WALStorageProvider replicateAndResendStorageProvider = new WALStorageProvider() {
            @Override
            public WALStorage create(File workingDirectory, String domain, RegionName regionName, WALReplicator walReplicator) throws Exception {
                File dir = new File(workingDirectory, domain + File.separator);
                dir.getParentFile().mkdirs();
                RowMarshaller<byte[]> rowMarshaller = new BinaryRowMarshaller();
                RowIOProvider rowIOProvider = new BinaryRowIOProvider();
                return new NonIndexWAL(regionName,
                    orderIdProvider,
                    rowMarshaller,
                    new BinaryWALTx(dir,
                        regionName.getRegionName() + ".kvt",
                        rowIOProvider,
                        rowMarshaller,
                        replicateAndResendWALIndexProvider,
                        100));
            }
        };

        AmzaStats amzaStats = new AmzaStats();
        MemoryBackedHighWaterMarks highWaterMarks = new MemoryBackedHighWaterMarks();
        AmzaService amzaService = new AmzaServiceInitializer().initialize(config,
            amzaStats,
            serviceHost,
            orderIdProvider,
            walStorageProvider,
            replicateAndResendStorageProvider,
            replicateAndResendStorageProvider,
            changeSetSender,
            taker,
            highWaterMarks,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(),
            new RowChanges() {

                @Override
                public void changes(RowsChanged changes) throws Exception {
                }
            });

        amzaService.start();

        //if (serviceHost.getPort() % 2 == 0) {
        final RegionName regionName = new RegionName("test", "region1", null, null);
        amzaService.watch(regionName, new RowChanges() {
            @Override
            public void changes(RowsChanged changes) throws Exception {
                if (changes.getApply().size() > 0) {
                    System.out.println("Service:" + serviceHost
                        + " Region:" + regionName.getRegionName()
                        + " Changed:" + changes.getApply().size());
                }
            }
        });
        //}

        amzaService.getAmzaRing().addRingHost("test", serviceHost); // ?? Hacky
        amzaService.getAmzaRing().addRingHost("MASTER", serviceHost); // ?? Hacky
        if (lastAmzaService != null) {
            amzaService.getAmzaRing().addRingHost("test", lastAmzaService.getAmzaRing().getRingHost()); // ?? Hacky
            amzaService.getAmzaRing().addRingHost("MASTER", lastAmzaService.getAmzaRing().getRingHost()); // ?? Hacky

            lastAmzaService.getAmzaRing().addRingHost("test", serviceHost); // ?? Hacky
            lastAmzaService.getAmzaRing().addRingHost("MASTER", serviceHost); // ?? Hacky
        }
        lastAmzaService = amzaService;

        service = new AmzaNode(serviceHost, amzaService, highWaterMarks);
        cluster.put(serviceHost, service);
        System.out.println("Added serviceHost:" + serviceHost + " to the cluster.");
        return service;
    }

    public class AmzaNode {

        private final Random random = new Random();
        private final RingHost serviceHost;
        private final AmzaService amzaService;
        private final MemoryBackedHighWaterMarks highWaterMarks;
        private boolean off = false;
        private int flapped = 0;

        public AmzaNode(RingHost serviceHost, AmzaService amzaService, MemoryBackedHighWaterMarks highWaterMarks) {
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

        void addToReplicatedWAL(RegionName mapName, WALScanable changes) throws Exception {
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

        public void takeRegion(RegionName regionName, long transationId, WALScan rowStream) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenTaking)) {
                throw new RuntimeException("Random take failure:" + serviceHost);
            }
            AmzaRegion got = amzaService.getRegion(regionName);
            if (got != null) {
                got.takeRowUpdatesSince(transationId, rowStream);
            }
        }

        public void printService() throws Exception {
            if (off) {
                System.out.println(serviceHost.getHost() + ":" + serviceHost.getPort() + " is OFF flapped:" + flapped);
                return;
            }
            amzaService.printService(serviceHost);
        }

        public boolean compare(AmzaNode service) throws Exception {
            if (off || service.off) {
                return true;
            }
            Map<RegionName, AmzaRegion> aRegions = amzaService.getRegions();
            Map<RegionName, AmzaRegion> bRegions = service.amzaService.getRegions();

            Set<RegionName> allARegions = aRegions.keySet();
            Set<RegionName> allBRegions = bRegions.keySet();
            if (allARegions.size() != allBRegions.size()) {
                System.out.println(allARegions + " -vs- " + allBRegions);
                return false;
            }

            Set<RegionName> regionNames = new HashSet<>();
            regionNames.addAll(allARegions);
            regionNames.addAll(allBRegions);

            for (RegionName regionName : regionNames) {
                AmzaRegion a = amzaService.getRegion(regionName);
                AmzaRegion b = service.amzaService.getRegion(regionName);
                if (!a.compare(b)) {
                    amzaService.printService(amzaService.getAmzaRing().getRingHost());
                    System.out.println("-- vs --");
                    service.amzaService.printService(service.amzaService.getAmzaRing().getRingHost());

                    System.out.println(highWaterMarks + " -vs- " + service.highWaterMarks);
                    return false;
                }
            }
            return true;
        }
    }
}
