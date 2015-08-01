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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.EmbeddedAmzaServiceInitializer;
import com.jivesoftware.os.amza.service.WALIndexProviderRegistry;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.shared.take.RowsTaker;
import com.jivesoftware.os.amza.shared.take.StreamingTakesConsumer;
import com.jivesoftware.os.amza.shared.take.StreamingTakesConsumer.StreamingTakeConsumed;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;

public class AmzaTestCluster {

    private final MetricLogger LOG = MetricLoggerFactory.getLogger();

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

    public AmzaNode newNode(final RingMember localRingMember, final RingHost localRingHost, PartitionName partitionName) throws Exception {

        AmzaNode service = cluster.get(localRingMember);
        if (service != null) {
            return service;
        }

        AmzaServiceConfig config = new AmzaServiceConfig();
        config.workingDirectories = new String[] { workingDirctory.getAbsolutePath() + "/" + localRingHost.getHost() + "-" + localRingHost.getPort() };
        config.compactTombstoneIfOlderThanNMillis = 100000L;
        //config.useMemMap = true;
        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        OrderIdProviderImpl orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(localRingHost.getPort()), idPacker,
            new JiveEpochTimestampProvider());

        AvailableRowsTaker availableRowsTaker = new AvailableRowsTaker() {

            @Override
            public void availableRowsStream(RingMember localRingMember,
                RingMember remoteRingMember,
                RingHost remoteRingHost,
                long takeSessionId,
                long timeoutMillis,
                AvailableRowsTaker.AvailableStream updatedPartitionsStream) throws Exception {

                AmzaNode amzaNode = cluster.get(remoteRingMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exists for " + remoteRingMember);
                } else {
                    amzaNode.takePartitionUpdates(localRingMember,
                        takeSessionId,
                        timeoutMillis,
                        (versionedPartitionName, status, txId) -> {
                            if (versionedPartitionName != null) {
                                updatedPartitionsStream.available(versionedPartitionName, status, txId);
                            }
                        },
                        () -> {
                            LOG.debug("Special delivery! Special delivery!");
                            return null;
                        },
                        () -> {
                            LOG.debug("Ping pong!");
                            return null;
                        });
                }
            }

        };

        RowsTaker updateTaker = new RowsTaker() {

            @Override
            public RowsTaker.StreamingRowsResult rowsStream(RingMember localRingMember,
                RingMember remoteRingMember,
                RingHost remoteRingHost,
                VersionedPartitionName remoteVersionedPartitionName,
                long remoteTxId,
                RowStream rowStream) {

                AmzaNode amzaNode = cluster.get(remoteRingMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exist for " + localRingMember);
                } else {
                    StreamingTakesConsumer.StreamingTakeConsumed consumed = amzaNode.rowsStream(localRingMember,
                        remoteVersionedPartitionName,
                        remoteTxId,
                        rowStream);
                    return new StreamingRowsResult(null, null, consumed.isOnline ? new HashMap<>() : null);
                }
            }

            @Override
            public boolean rowsTaken(RingMember localRingMember,
                RingMember remoteRingMember,
                RingHost remoteRingHost,
                VersionedPartitionName remoteVersionedPartitionName,
                long remoteTxId) {
                AmzaNode amzaNode = cluster.get(remoteRingMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exists for " + localRingMember);
                } else {
                    try {
                        amzaNode.remoteMemberTookToTxId(localRingMember, remoteVersionedPartitionName, remoteTxId);
                        return true;
                    } catch (Exception x) {
                        throw new RuntimeException("Issue while applying acks.", x);
                    }
                }
            }
        };

        final ObjectMapper mapper = new ObjectMapper();
        PartitionPropertyMarshaller partitionPropertyMarshaller = new PartitionPropertyMarshaller() {

            @Override
            public PartitionProperties fromBytes(byte[] bytes) {
                try {
                    return mapper.readValue(bytes, PartitionProperties.class);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }

            @Override
            public byte[] toBytes(PartitionProperties partitionProperties) {
                try {
                    return mapper.writeValueAsBytes(partitionProperties);
                } catch (JsonProcessingException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };

        AmzaStats amzaStats = new AmzaStats();
        Optional<TakeFailureListener> absent = Optional.<TakeFailureListener>absent();

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(config,
            amzaStats,
            localRingMember,
            localRingHost,
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            new WALIndexProviderRegistry(),
            availableRowsTaker, () -> {
                return updateTaker;
            },
            absent,
            (RowsChanged changes) -> {
            });

        amzaService.start();

        amzaService.watch(partitionName,
            (RowsChanged changes) -> {
                /*if (changes.getApply().size() > 0) {
                 System.out.println("Service:" + localRingMember
                 + " Partition:" + partitionName.getName()
                 + " Changed:" + changes.getApply().size());
                 }*/
            }
        );

        try {
            amzaService.getRingWriter().addRingMember(AmzaRingReader.SYSTEM_RING, localRingMember); // ?? Hacky
            amzaService.getRingWriter().addRingMember("test".getBytes(), localRingMember); // ?? Hacky
            if (lastAmzaService != null) {
                amzaService.getRingWriter().register(lastAmzaService.getRingReader().getRingMember(), lastAmzaService.getRingWriter().getRingHost());
                amzaService.getRingWriter().addRingMember(AmzaRingReader.SYSTEM_RING, lastAmzaService.getRingReader().getRingMember()); // ?? Hacky
                amzaService.getRingWriter().addRingMember("test".getBytes(), lastAmzaService.getRingReader().getRingMember()); // ?? Hacky

                lastAmzaService.getRingWriter().register(localRingMember, localRingHost);
                lastAmzaService.getRingWriter().addRingMember(AmzaRingReader.SYSTEM_RING, localRingMember); // ?? Hacky
                lastAmzaService.getRingWriter().addRingMember("test".getBytes(), localRingMember); // ?? Hacky
            }
            lastAmzaService = amzaService;
        } catch (Exception x) {
            x.printStackTrace();
            System.out.println("FAILED CONNECTING RING");
            System.exit(1);
        }

        service = new AmzaNode(localRingMember, localRingHost, amzaService, orderIdProvider);

        cluster.put(localRingMember, service);

        System.out.println("Added serviceHost:" + localRingMember + " to the cluster.");
        return service;
    }

    public class AmzaNode {

        private final Random random = new Random();
        private final RingMember ringMember;
        private final RingHost ringHost;
        private final AmzaService amzaService;
        private final TimestampedOrderIdProvider orderIdProvider;
        private boolean off = false;
        private int flapped = 0;
        private final ExecutorService asIfOverTheWire = Executors.newSingleThreadExecutor();
        private final AmzaKretrProvider clientProvider;

        public AmzaNode(RingMember ringMember,
            RingHost ringHost,
            AmzaService amzaService,
            TimestampedOrderIdProvider orderIdProvider) {

            this.ringMember = ringMember;
            this.ringHost = ringHost;
            this.amzaService = amzaService;
            this.clientProvider = new AmzaKretrProvider(amzaService);
            this.orderIdProvider = orderIdProvider;
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

            amzaService.setPropertiesIfAbsent(partitionName, new PartitionProperties(storageDescriptor, 2, false));
            amzaService.awaitOnline(partitionName, 10_000);
        }

        public void update(PartitionName partitionName, byte[] p, byte[] k, byte[] v, boolean tombstone) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }

            AmzaPartitionUpdates updates = new AmzaPartitionUpdates();
            long timestamp = orderIdProvider.nextId();
            if (tombstone) {
                updates.remove(p, k, timestamp);
            } else {
                updates.set(p, k, v, timestamp);
            }
            clientProvider.getClient(partitionName).commit(updates, 2, 10, TimeUnit.SECONDS);

        }

        public byte[] get(PartitionName partitionName, byte[] prefix, byte[] key) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }

            List<byte[]> got = new ArrayList<>();
            clientProvider.getClient(partitionName).get(stream -> stream.stream(prefix, key),
                (_prefix, _key, value, timestamp) -> {
                    got.add(value);
                    return true;
                });
            return got.get(0);
        }

        void remoteMemberTookToTxId(RingMember remoteRingMember,
            VersionedPartitionName remoteVersionedPartitionName,
            long localTxId) throws Exception {
            amzaService.rowsTaken(remoteRingMember, remoteVersionedPartitionName, localTxId);
        }

        public StreamingTakeConsumed rowsStream(RingMember remoteRingMember,
            VersionedPartitionName localVersionedPartitionName,
            long localTxId,
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
                    amzaService.rowsStream(new DataOutputStream(bytesOut), remoteRingMember, localVersionedPartitionName, localTxId);
                    return null;
                });
                submit.get();
                StreamingTakesConsumer streamingTakesConsumer = new StreamingTakesConsumer();
                return streamingTakesConsumer.consume(new ByteArrayInputStream(bytesOut.toByteArray()), rowStream);
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

            NavigableMap<RingMember, RingHost> aRing = amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING);
            NavigableMap<RingMember, RingHost> bRing = service.amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING);

            if (!aRing.equals(bRing)) {
                System.out.println(aRing + "-vs-" + bRing);
                return false;
            }

            for (PartitionName partitionName : partitionNames) {
                if (partitionName.equals(PartitionProvider.HIGHWATER_MARK_INDEX.getPartitionName())) {
                    continue;
                }

                AmzaPartitionAPI a = amzaService.getPartition(partitionName);
                AmzaPartitionAPI b = service.amzaService.getPartition(partitionName);
                if (a == null || b == null) {
                    System.out.println(partitionName + " " + amzaService.getRingReader().getRingMember() + " " + a + " -- vs --"
                        + service.amzaService.getRingReader().getRingMember() + " " + b);
                    return false;
                }
                if (!compare(partitionName, a, b)) {
                    return false;
                }
            }
            return true;
        }

        private void takePartitionUpdates(RingMember ringMember,
            long sessionId,
            long timeoutMillis,
            AvailableRowsTaker.AvailableStream updatedPartitionsStream,
            Callable<Void> deliverCallback,
            Callable<Void> pingCallback) {

            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenTaking)) {
                throw new RuntimeException("Random take failure:" + ringMember);
            }

            try {
                amzaService.availableRowsStream(ringMember, sessionId, timeoutMillis, updatedPartitionsStream, deliverCallback, pingCallback);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        //  Use for testing
        private boolean compare(PartitionName partitionName, AmzaPartitionAPI a, AmzaPartitionAPI b) throws Exception {
            final MutableInt compared = new MutableInt(0);
            final MutableBoolean passed = new MutableBoolean(true);
            a.scan(null, null, null, null, (txid, prefix, key, aValue) -> {
                try {
                    compared.increment();
                    TimestampedValue[] bValues = new TimestampedValue[1];
                    b.get(stream -> stream.stream(prefix, key),
                        (_prefix, _key, value, timestamp) -> {
                            bValues[0] = new TimestampedValue(timestamp, value);
                            return true;
                        });

                    TimestampedValue bValue = bValues[0];
                    String comparing = new String(partitionName.getRingName()) + ":" + new String(partitionName.getName())
                        + " to " + new String(partitionName.getRingName()) + ":" + new String(partitionName.getName()) + "\n";

                    if (bValue == null) {
                        System.out.println("INCONSISTENCY: " + comparing + " " + aValue.getTimestampId()
                            + " != null"
                            + "' \n" + aValue + " vs null");
                        passed.setValue(false);
                        return false;
                    }
                    if (aValue.getTimestampId() != bValue.getTimestampId()) {
                        System.out.println("INCONSISTENCY: " + comparing + " timestamp:'" + aValue.getTimestampId()
                            + "' != '" + bValue.getTimestampId()
                            + "' \n" + aValue + " vs " + bValue);
                        passed.setValue(false);
                        System.out.println("----------------------------------");

                        return false;
                    }
                    if (aValue.getValue() == null && bValue.getValue() != null) {
                        System.out.println("INCONSISTENCY: " + comparing + " null"
                            + " != '" + Arrays.toString(bValue.getValue())
                            + "' \n" + aValue + " vs " + bValue);
                        passed.setValue(false);
                        return false;
                    }
                    if (aValue.getValue() != null && !Arrays.equals(aValue.getValue(), bValue.getValue())) {
                        System.out.println("INCONSISTENCY: " + comparing + " value:'" + Arrays.toString(aValue.getValue())
                            + "' != '" + Arrays.toString(bValue.getValue())
                            + "' \n" + aValue + " vs " + bValue);
                        passed.setValue(false);
                        return false;
                    }
                    return true;
                } catch (Exception x) {
                    throw new RuntimeException("Failed while comparing", x);
                }
            });

            System.out.println(
                "partition:" + new String(partitionName.getName()) + " vs:" + new String(partitionName.getName()) + " compared:" + compared + " keys");
            return passed.booleanValue();
        }
    }
}
