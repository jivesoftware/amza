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
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.service.AmzaChangeIdPacker;
import com.jivesoftware.os.amza.service.AmzaPartition;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighwaterStorage;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.take.StreamingTakesConsumer;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AmzaTestCluster {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final TimestampedOrderIdProvider ORDER_ID_PROVIDER = new OrderIdProviderImpl(new ConstantWriterIdProvider(1),
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
        config.workingDirectories = new String[] { workingDirctory.getAbsolutePath() + "/" + ringHost.getHost() + "-" + ringHost.getPort() };
        config.takeFromNeighborsIntervalInMillis = 5;
        config.compactTombstoneIfOlderThanNMillis = 100000L;

        UpdatesTaker updateTaker = new UpdatesTaker() {

            @Override
            public boolean ackTakenUpdate(RingMember ringMember, RingHost ringHost, Collection<UpdatesTaker.AckTaken> ackTaken) {
                AmzaNode amzaNode = cluster.get(ringMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exists for " + ringMember);
                } else {
                    try {
                        amzaNode.takeAcks(ringMember, ringHost, (AmzaInstance.AcksStream acksStream) -> {
                            for (AckTaken a : ackTaken) {
                                acksStream.stream(a.partitionName, a.txId);
                            }
                        });
                        return true;
                    } catch (Exception x) {
                        throw new RuntimeException("Issue while applying acks.", x);
                    }
                }
            }

            @Override
            public StreamingTakeResult streamingTakeUpdates(RingMember taker, RingHost takerHost, Map.Entry<RingMember, RingHost> node,
                PartitionName partitionName, long transactionId, RowStream tookRowUpdates) {
                AmzaNode amzaNode = cluster.get(node.getKey());
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exists for " + node.getValue());
                } else {
                    StreamingTakesConsumer.StreamingTakeConsumed consumed = amzaNode.takePartition(taker,
                        takerHost,
                        partitionName,
                        transactionId,
                        tookRowUpdates);
                    return new StreamingTakeResult(consumed.partitionVersion, null, null, consumed.isOnline ? new HashMap<>() : null);
                }
            }
        };

        // TODO need to get writer id from somewhere other than port.
        final TimestampedOrderIdProvider orderIdProvider = ORDER_ID_PROVIDER;

        final ObjectMapper mapper = new ObjectMapper();
        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) throws Exception {
                return mapper.readValue(bytes, PartitionProperties.class);
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) throws Exception {
                return mapper.writeValueAsBytes(partitionProperties);
            }
        };

        AmzaStats amzaStats = new AmzaStats();
        HighwaterStorage highWaterMarks = new MemoryBackedHighwaterStorage();

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(config,
            amzaStats,
            ringMember,
            ringHost,
            orderIdProvider,
            partitionPropertyMarshaller,
            new WALIndexProviderRegistry(),
            updateTaker,
            Optional.<SendFailureListener>absent(),
            Optional.<TakeFailureListener>absent(), (RowsChanged changes) -> {
            });

        amzaService.start();

        final PartitionName partitionName = new PartitionName(false, "test", "partition1");

        amzaService.watch(partitionName,
            (RowsChanged changes) -> {
                if (changes.getApply().size() > 0) {
                    System.out.println("Service:" + ringMember
                        + " Partition:" + partitionName.getPartitionName()
                        + " Changed:" + changes.getApply().size());
                }
            }
        );

        try {
            amzaService.getAmzaHostRing().addRingMember("system", ringMember); // ?? Hacky
            amzaService.getAmzaHostRing().addRingMember("test", ringMember); // ?? Hacky
            if (lastAmzaService != null) {
                amzaService.getAmzaHostRing().register(lastAmzaService.getAmzaHostRing().getRingMember(), lastAmzaService.getAmzaHostRing().getRingHost());
                amzaService.getAmzaHostRing().addRingMember("system", lastAmzaService.getAmzaHostRing().getRingMember()); // ?? Hacky
                amzaService.getAmzaHostRing().addRingMember("test", lastAmzaService.getAmzaHostRing().getRingMember()); // ?? Hacky

                lastAmzaService.getAmzaHostRing().register(ringMember, ringHost);
                lastAmzaService.getAmzaHostRing().addRingMember("system", ringMember); // ?? Hacky
                lastAmzaService.getAmzaHostRing().addRingMember("test", ringMember); // ?? Hacky
            }
            lastAmzaService = amzaService;
        } catch (Exception x) {
            x.printStackTrace();
            System.out.println("FAILED CONNECTING RING");
            System.exit(1);
        }

        service = new AmzaNode(ringMember, ringHost, amzaService, highWaterMarks);

        cluster.put(ringMember, service);

        System.out.println(
            "Added serviceHost:" + ringMember + " to the cluster.");
        return service;
    }

    public class AmzaNode {

        private final Random random = new Random();
        private final RingMember ringMember;
        private final RingHost ringHost;
        private final AmzaService amzaService;
        private final HighwaterStorage highWaterMarks;
        private boolean off = false;
        private int flapped = 0;
        private final ExecutorService asIfOverTheWire = Executors.newSingleThreadExecutor();
        private final AmzaKretrProvider clientProvider;

        public AmzaNode(RingMember ringMember,
            RingHost ringHost,
            AmzaService amzaService,
            HighwaterStorage highWaterMarks) {

            this.ringMember = ringMember;
            this.ringHost = ringHost;
            this.amzaService = amzaService;
            this.highWaterMarks = highWaterMarks;
            this.clientProvider = new AmzaKretrProvider(amzaService);
        }

        @Override
        public String toString() {
            return ringMember.toString();
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

        public void create(PartitionName partitionName) throws Exception {
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);

            amzaService.setPropertiesIfAbsent(partitionName, new PartitionProperties(storageDescriptor, 2, 2, false));

            AmzaService.AmzaPartitionRoute partitionRoute = amzaService.getPartitionRoute(partitionName);
            while (partitionRoute.orderedPartitionHosts.isEmpty()) {
                LOG.info("Waiting for " + partitionName + " to come online.");
                Thread.sleep(100);
                partitionRoute = amzaService.getPartitionRoute(partitionName);
            }
        }

        public void update(PartitionName partitionName, WALKey k, byte[] v, long timestamp, boolean tombstone) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }

            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            if (tombstone) {
                updates.remove(k, timestamp);
            } else {
                updates.set(k, v, timestamp);
            }
            clientProvider.getClient(partitionName).commit(updates, 2, 10, TimeUnit.SECONDS);

        }

        public byte[] get(PartitionName partitionName, WALKey key) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }

            List<byte[]> got = new ArrayList<>();
            clientProvider.getClient(partitionName).get(Collections.singletonList(key), (rowTxId, key1, timestampedValue) -> {
                got.add(timestampedValue != null ? timestampedValue.getValue() : null);
                return true;
            });
            return got.get(0);
        }

        void takeAcks(RingMember ringMember, RingHost ringHost, AmzaInstance.StreamableAcks acks) throws Exception {
            amzaService.takeAcks(ringMember, ringHost, acks);
        }

        public StreamingTakesConsumer.StreamingTakeConsumed takePartition(RingMember takerRingMember,
            RingHost takerRingHost,
            PartitionName partitionName,
            long transactionId,
            RowStream rowStream) {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenTaking)) {
                throw new RuntimeException("Random take failure:" + ringMember);
            }

            try {
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                Future<Object> submit = asIfOverTheWire.submit(() -> {
                    amzaService.streamingTakeFromPartition(new DataOutputStream(bytesOut), takerRingMember, takerRingHost, partitionName, transactionId);
                    return null;
                });
                submit.get();
                StreamingTakesConsumer streamingTakesConsumer = new StreamingTakesConsumer();
                StreamingTakesConsumer.StreamingTakeConsumed consumed = streamingTakesConsumer.consume(new ByteArrayInputStream(bytesOut.toByteArray()),
                    rowStream);
                /*
                 (long rowFP, long rowTxId, RowType rowType, byte[] row) -> {
                 if (!partitionName.isSystemPartition()) {
                 if (rowType == RowType.primary) {
                 BinaryPrimaryRowMarshaller binaryPrimaryRowMarshaller = new BinaryPrimaryRowMarshaller();
                 System.out.println("TOOK:" + takerRingMember + "." + partitionName +
                 " FROM:" + ringMember + " VALUE:" + binaryPrimaryRowMarshaller.fromRow(row));
                 }
                 }
                 return rowStream.row(rowFP, rowTxId, rowType, row);
                 });
                 */
                return consumed;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void printService() throws Exception {
            if (off) {
                System.out.println(ringHost.getHost() + ":" + ringHost.getPort() + " is OFF flapped:" + flapped);
            }
        }

        public boolean compare(AmzaNode service) throws Exception {
            if (off || service.off) {
                return true;
            }

            Set<PartitionName> allAPartitions = amzaService.getPartitionNames();
            Set<PartitionName> allBPartitions = service.amzaService.getPartitionNames();

            if (allAPartitions.size() != allBPartitions.size()) {
                System.out.println(allAPartitions + " -vs- " + allBPartitions);
                return false;
            }

            Set<PartitionName> partitionNames = new HashSet<>();
            partitionNames.addAll(allAPartitions);
            partitionNames.addAll(allBPartitions);

            NavigableMap<RingMember, RingHost> aRing = amzaService.getAmzaRingReader().getRing("system");
            NavigableMap<RingMember, RingHost> bRing = service.amzaService.getAmzaRingReader().getRing("system");

            if (!aRing.equals(bRing)) {
                System.out.println(aRing + "-vs-" + bRing);
                return false;
            }

            for (PartitionName partitionName : partitionNames) {
                if (partitionName.equals(PartitionProvider.HIGHWATER_MARK_INDEX.getPartitionName())) {
                    continue;
                }

                AmzaPartition a = amzaService.getPartition(partitionName);
                AmzaPartition b = service.amzaService.getPartition(partitionName);
                if (a == null || b == null) {
                    System.out.println(partitionName + " " + amzaService.getAmzaHostRing().getRingMember() + " " + a + " -- vs --"
                        + service.amzaService.getAmzaHostRing().getRingMember() + " " + b);
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
