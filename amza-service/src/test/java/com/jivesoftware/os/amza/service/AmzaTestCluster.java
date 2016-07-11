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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.take.TakeCursors;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexProvider;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexConfig;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexProvider;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.PartitionPropertyMarshaller;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.amza.service.take.StreamingTakesConsumer;
import com.jivesoftware.os.amza.service.take.StreamingTakesConsumer.StreamingTakeConsumed;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import org.merlin.config.BindInterfaceToConfiguration;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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

    public AmzaNode newNode(final RingMember localRingMember, final RingHost localRingHost) throws Exception {

        AmzaNode service = cluster.get(localRingMember);
        if (service != null) {
            return service;
        }

        AmzaServiceConfig config = new AmzaServiceConfig();
        config.workingDirectories = new String[]{workingDirctory.getAbsolutePath() + "/" + localRingHost.getHost() + "-" + localRingHost.getPort()};
        config.aquariumLivelinessFeedEveryMillis = 500;
        config.maxUpdatesBeforeDeltaStripeCompaction = 10;
        config.deltaStripeCompactionIntervalInMillis = 1000;
        config.flushHighwatersAfterNUpdates = 10;

        config.initialBufferSegmentSize = 1_024;
        config.maxBufferSegmentSize = 10 * 1_024;

        config.updatesBetweenLeaps = 10;
        config.useMemMap = true;

        SnowflakeIdPacker idPacker = new SnowflakeIdPacker();
        OrderIdProviderImpl orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(localRingHost.getPort()), idPacker,
            new JiveEpochTimestampProvider());

        AvailableRowsTaker availableRowsTaker =
            (localRingMember1, localTimestampedRingHost, remoteRingMember, remoteRingHost, system, takeSessionId, timeoutMillis, updatedPartitionsStream) -> {

                AmzaNode amzaNode = cluster.get(remoteRingMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exist for " + remoteRingMember);
                } else {
                    amzaNode.takePartitionUpdates(localRingMember1,
                        localTimestampedRingHost,
                        system,
                        takeSessionId,
                        timeoutMillis,
                        (versionedPartitionName, txId) -> {
                            if (versionedPartitionName != null) {
                                updatedPartitionsStream.available(versionedPartitionName, txId);
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
            };

        RowsTaker updateTaker = new RowsTaker() {

            @Override
            public RowsTaker.StreamingRowsResult rowsStream(RingMember localRingMember,
                RingMember remoteRingMember,
                RingHost remoteRingHost,
                VersionedPartitionName remoteVersionedPartitionName,
                long remoteTxId,
                long localLeadershipToken,
                RowStream rowStream) {

                AmzaNode amzaNode = cluster.get(remoteRingMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exist for " + localRingMember);
                } else {
                    StreamingTakesConsumer.StreamingTakeConsumed consumed = amzaNode.rowsStream(localRingMember,
                        remoteVersionedPartitionName,
                        remoteTxId,
                        localLeadershipToken,
                        rowStream);
                    HashMap<RingMember, Long> otherHighwaterMarks = consumed.isOnline ? new HashMap<>() : null;
                    return new StreamingRowsResult(null, null, consumed.leadershipToken, consumed.partitionVersion, otherHighwaterMarks);
                }
            }

            @Override
            public boolean rowsTaken(RingMember localRingMember,
                RingMember remoteRingMember,
                RingHost remoteRingHost,
                long takeSessionId,
                VersionedPartitionName remoteVersionedPartitionName,
                long remoteTxId,
                long localLeadershipToken) {
                AmzaNode amzaNode = cluster.get(remoteRingMember);
                if (amzaNode == null) {
                    throw new IllegalStateException("Service doesn't exists for " + localRingMember);
                } else {
                    try {
                        amzaNode.remoteMemberTookToTxId(localRingMember, takeSessionId, remoteVersionedPartitionName, remoteTxId, localLeadershipToken);
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

        BAInterner interner = new BAInterner();
        AmzaStats amzaStats = new AmzaStats();
        SickThreads sickThreads = new SickThreads();
        SickPartitions sickPartitions = new SickPartitions();
        Optional<TakeFailureListener> absent = Optional.<TakeFailureListener>absent();

        AmzaService amzaService = new EmbeddedAmzaServiceInitializer().initialize(config,
            interner,
            amzaStats,
            sickThreads,
            sickPartitions,
            localRingMember,
            localRingHost,
            Collections.emptySet(),
            orderIdProvider,
            idPacker,
            partitionPropertyMarshaller,
            (workingIndexDirectories, indexProviderRegistry, ephemeralRowIOProvider, persistentRowIOProvider, partitionStripeFunction) -> {

                indexProviderRegistry.register(
                    new BerkeleyDBWALIndexProvider(BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME, partitionStripeFunction, workingIndexDirectories),
                    persistentRowIOProvider);

                LABPointerIndexConfig labConfig = BindInterfaceToConfiguration.bindDefault(LABPointerIndexConfig.class);

                indexProviderRegistry.register(new LABPointerIndexWALIndexProvider(labConfig,
                    LABPointerIndexWALIndexProvider.INDEX_CLASS_NAME,
                    partitionStripeFunction,
                    workingIndexDirectories),
                    persistentRowIOProvider);

            },
            availableRowsTaker,
            () -> updateTaker,
            absent,
            (RowsChanged changes) -> {
            });

        amzaService.start(localRingMember, localRingHost);

        try {
            //amzaService.getRingWriter().addRingMember(AmzaRingReader.SYSTEM_RING, localRingMember); // ?? Hacky
            TimestampedRingHost timestampedRingHost = amzaService.getRingReader().getRingHost();
            amzaService.getRingWriter().addRingMember("test".getBytes(), localRingMember); // ?? Hacky
            if (lastAmzaService != null) {
                TimestampedRingHost lastTimestampedRingHost = lastAmzaService.getRingReader().getRingHost();
                amzaService.getRingWriter().register(lastAmzaService.getRingReader().getRingMember(),
                    lastTimestampedRingHost.ringHost,
                    lastTimestampedRingHost.timestampId);
                amzaService.getRingWriter().addRingMember("test".getBytes(), lastAmzaService.getRingReader().getRingMember()); // ?? Hacky

                lastAmzaService.getRingWriter().register(localRingMember, localRingHost, timestampedRingHost.timestampId);
                lastAmzaService.getRingWriter().addRingMember("test".getBytes(), localRingMember); // ?? Hacky
            }
            lastAmzaService = amzaService;
        } catch (Exception x) {
            x.printStackTrace();
            System.out.println("FAILED CONNECTING RING");
            System.exit(1);
        }

        
        service = new AmzaNode(interner, localRingMember, localRingHost, amzaService, orderIdProvider, sickThreads, sickPartitions);

        cluster.put(localRingMember, service);

        System.out.println("Added serviceHost:" + localRingMember + " to the cluster.");
        return service;
    }

    public class AmzaNode {

        private final Random random = new Random();
        final RingMember ringMember;
        final RingHost ringHost;
        private final AmzaService amzaService;
        private final TimestampedOrderIdProvider orderIdProvider;
        final SickThreads sickThreads;
        final SickPartitions sickPartitions;
        private boolean off = false;
        private int flapped = 0;
        private final ExecutorService asIfOverTheWire = Executors.newSingleThreadExecutor();
        private final EmbeddedClientProvider clientProvider;
        private final BAInterner interner;

        public AmzaNode(BAInterner interner,
            RingMember ringMember,
            RingHost ringHost,
            AmzaService amzaService,
            TimestampedOrderIdProvider orderIdProvider,
            SickThreads sickThreads,
            SickPartitions sickPartitions) {

            this.interner = interner;
            this.ringMember = ringMember;
            this.ringHost = ringHost;
            this.amzaService = amzaService;
            this.clientProvider = new EmbeddedClientProvider(amzaService);
            this.orderIdProvider = orderIdProvider;
            this.sickThreads = sickThreads;
            this.sickPartitions = sickPartitions;
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
            asIfOverTheWire.shutdownNow();
        }

        public void create(Consistency consistency,
            PartitionName partitionName,
            String indexClassName,
            int maxValueSizeInIndex,
            RowType rowType) throws Exception {
            // TODO test other consistencies and durabilities and .... Hehe
            PartitionProperties properties = new PartitionProperties(Durability.fsync_never,
                0, 0, 0, 0, 0, 0, 0, 0,
                false,
                consistency,
                true,
                true,
                false,
                rowType,
                indexClassName,
                maxValueSizeInIndex,
                null,
                -1,
                -1);
            amzaService.createPartitionIfAbsent(partitionName, properties);
            amzaService.awaitOnline(partitionName, Integer.MAX_VALUE); //TODO lololol
        }

        public void update(Consistency consistency, PartitionName partitionName, byte[] p, byte[] k, byte[] v, boolean tombstone) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }

            clientProvider.getClient(partitionName).commit(consistency, p, commitKeyValueStream -> {
                long timestamp = orderIdProvider.nextId();
                if (tombstone) {
                    return commitKeyValueStream.commit(k, null, timestamp, true);
                } else {
                    return commitKeyValueStream.commit(k, v, timestamp, false);
                }
            }, 10, TimeUnit.SECONDS);

        }

        public byte[] get(Consistency consistency, PartitionName partitionName, byte[] prefix, byte[] key) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }

            List<byte[]> got = new ArrayList<>();
            clientProvider.getClient(partitionName).get(consistency, prefix, stream -> stream.stream(key),
                (_prefix, _key, value, timestamp, version) -> {
                    got.add(value);
                    return true;
                });
            return got.get(0);
        }

        public TakeCursors takeFromTransactionId(PartitionName partitionName, long transactionId, TxKeyValueStream stream) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + ringMember);
            }
            return clientProvider.getClient(partitionName).takeFromTransactionId(transactionId, stream);
        }

        public void watch(PartitionName partitionName) throws Exception {
            amzaService.watch(partitionName,
                (RowsChanged changes) -> {
                    /*if (changes.getApply().size() > 0) {
                     System.out.println("Service:" + localRingMember
                     + " Partition:" + partitionName.getName()
                     + " Changed:" + changes.getApply().size());
                     }*/
                }
            );
        }

        void remoteMemberTookToTxId(RingMember remoteRingMember,
            long takeSessionId,
            VersionedPartitionName remoteVersionedPartitionName,
            long localTxId,
            long leadershipToken) throws Exception {
            amzaService.rowsTaken(remoteRingMember, takeSessionId, remoteVersionedPartitionName, localTxId, leadershipToken);
        }

        public StreamingTakeConsumed rowsStream(RingMember remoteRingMember,
            VersionedPartitionName localVersionedPartitionName,
            long localTxId,
            long leadershipToken,
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
                    DataOutputStream dos = new DataOutputStream(new SnappyOutputStream(bytesOut));
                    amzaService.rowsStream(dos,
                        remoteRingMember,
                        localVersionedPartitionName,
                        localTxId,
                        leadershipToken);
                    dos.flush();
                    return null;
                });
                submit.get();

                StreamingTakesConsumer streamingTakesConsumer = new StreamingTakesConsumer(interner);
                return streamingTakesConsumer.consume(new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(bytesOut.toByteArray()))), rowStream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void printService() throws Exception {
            if (off) {
                System.out.println(ringHost.getHost() + ":" + ringHost.getPort() + " is OFF flapped:" + flapped);
            }
        }

        public void printRings() {
            try {
                RingTopology systemRing = amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING);
                System.out.println("RING:"
                    + " me:" + amzaService.getRingReader().getRingMember()
                    + " ring:" + Lists.transform(systemRing.entries, input -> input.ringMember));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public boolean isEmpty() throws Exception {
            Set<PartitionName> allAPartitions = Sets.newHashSet(amzaService.getAllPartitionNames());
            if (allAPartitions.isEmpty()) {
                return true;
            }

            for (PartitionName partitionName : allAPartitions) {
                if (!partitionName.isSystemPartition()) {
                    Partition partition = amzaService.getPartition(partitionName);
                    int[] count = {0};
                    partition.scan(Collections.singletonList(ScanRange.ROW_SCAN), (prefix, key, value, timestamp, version) -> {
                        count[0]++;
                        return true;
                    });
                    if (count[0] > 0) {
                        return false;
                    }
                }
            }
            return true;
        }

        private final Set<PartitionName> IGNORED_PARTITION_NAMES = ImmutableSet.of(PartitionCreator.HIGHWATER_MARK_INDEX.getPartitionName(),
            PartitionCreator.AQUARIUM_LIVELINESS_INDEX.getPartitionName());

        public boolean compare(Consistency consistency, AmzaNode service) throws Exception {
            if (off || service.off) {
                return true;
            }

            Set<PartitionName> allAPartitions = Sets.newHashSet();
            Iterables.addAll(allAPartitions, amzaService.getSystemPartitionNames());
            Iterables.addAll(allAPartitions, amzaService.getMemberPartitionNames());

            Set<PartitionName> allBPartitions = Sets.newHashSet(service.amzaService.getAllPartitionNames());
            Iterables.addAll(allBPartitions, service.amzaService.getSystemPartitionNames());
            Iterables.addAll(allBPartitions, service.amzaService.getMemberPartitionNames());

            if (allAPartitions.size() != allBPartitions.size()) {
                System.out.println(allAPartitions + " -vs- " + allBPartitions);
                return false;
            }

            Set<PartitionName> partitionNames = new HashSet<>();
            partitionNames.addAll(allAPartitions);
            partitionNames.addAll(allBPartitions);

            RingTopology aRing = amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING);
            RingTopology bRing = service.amzaService.getRingReader().getRing(AmzaRingReader.SYSTEM_RING);

            if (!aRing.entries.equals(bRing.entries)) {
                System.out.println(aRing + "-vs-" + bRing);
                return false;
            }

            for (PartitionName partitionName : partitionNames) {
                if (IGNORED_PARTITION_NAMES.contains(partitionName)) {
                    continue;
                }

                Partition a = amzaService.getPartition(partitionName);
                Partition b = service.amzaService.getPartition(partitionName);
                if (a == null || b == null) {
                    System.out.println(partitionName + " " + amzaService.getRingReader().getRingMember() + " " + a + " -- vs --"
                        + service.amzaService.getRingReader().getRingMember() + " " + b);
                    return false;
                }
                if (!compare(consistency, partitionName, a, b)) {
                    return false;
                }
            }
            return true;
        }

        private void takePartitionUpdates(RingMember ringMember,
            TimestampedRingHost timestampedRingHost,
            boolean system,
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
                amzaService.availableRowsStream(system,
                    ringMember,
                    timestampedRingHost,
                    sessionId,
                    timeoutMillis,
                    updatedPartitionsStream,
                    deliverCallback,
                    pingCallback);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        //  Use for testing
        private boolean compare(Consistency consistency, PartitionName partitionName, Partition a, Partition b) throws Exception {
            final MutableInt compared = new MutableInt(0);
            final MutableBoolean passed = new MutableBoolean(true);
            try {
                a.scan(Collections.singletonList(ScanRange.ROW_SCAN),
                    (prefix, key, aValue, aTimestamp, aVersion) -> {
                        try {
                            compared.increment();
                            long[] btimestamp = new long[1];
                            byte[][] bvalue = new byte[1][];
                            long[] bversion = new long[1];
                            b.get(consistency, prefix, stream -> stream.stream(key),
                                (_prefix, _key, value, timestamp, tombstoned, version) -> {
                                    if (timestamp != -1 && !tombstoned) {
                                        btimestamp[0] = timestamp;
                                        bvalue[0] = value;
                                        bversion[0] = version;
                                    }
                                    return true;
                                });

                            long bTimetamp = btimestamp[0];
                            byte[] bValue = bvalue[0];
                            long bVersion = bversion[0];
                            String comparing = new String(partitionName.getRingName()) + ":" + new String(partitionName.getName())
                            + " to " + new String(partitionName.getRingName()) + ":" + new String(partitionName.getName()) + "\n";

                            if (bValue == null) {
                                System.out.println("INCONSISTENCY: " + comparing + " " + Arrays.toString(aValue)
                                    + " != null"
                                    + "' \n" + Arrays.toString(aValue) + " vs null");
                                passed.setValue(false);
                                return false;
                            }
                            if (aTimestamp != bTimetamp) {
                                System.out.println("INCONSISTENCY: " + comparing + " timestamp:'" + aTimestamp
                                    + "' != '" + bTimetamp
                                    + "' \n" + Arrays.toString(aValue) + " vs " + Arrays.toString(bValue));
                                passed.setValue(false);
                                System.out.println("----------------------------------");
                                return false;
                            }
                            if (aVersion != bVersion) {
                                System.out.println("INCONSISTENCY: " + comparing + " version:'" + aVersion
                                    + "' != '" + bVersion
                                    + "' \n" + Arrays.toString(aValue) + " vs " + Arrays.toString(bValue));
                                passed.setValue(false);
                                System.out.println("----------------------------------");
                                return false;
                            }
                            if (aValue == null && bValue != null) {
                                System.out.println("INCONSISTENCY: " + comparing + " null"
                                    + " != '" + Arrays.toString(bValue)
                                    + "' \n" + "null" + " vs " + Arrays.toString(bValue));
                                passed.setValue(false);
                                return false;
                            }
                            if (aValue != null && !Arrays.equals(aValue, bValue)) {
                                System.out.println("INCONSISTENCY: " + comparing + " value:'" + Arrays.toString(aValue)
                                    + "' != '" + Arrays.toString(bValue)
                                    + "' \n" + Arrays.toString(aValue) + " vs " + Arrays.toString(bValue));
                                passed.setValue(false);
                                return false;
                            }
                            return true;
                        } catch (Exception x) {
                            throw new RuntimeException("Failed while comparing", x);
                        }
                    });
            } catch (Exception e) {
                System.out.println("EXCEPTION: " + e.getMessage());
                e.printStackTrace();
                passed.setValue(false);
            }

            System.out.println(
                "partition:" + new String(partitionName.getName()) + " vs:" + new String(partitionName.getName()) + " compared:" + compared + " keys");
            return passed.booleanValue();
        }

        public AmzaService.AmzaPartitionRoute getPartitionRoute(PartitionName partitionName) throws Exception {
            return amzaService.getPartitionRoute(partitionName, 0);
        }

        public void compactAllTombstones() throws Exception {
            LOG.info("Manual compact all tombstones requests.");
            amzaService.compactAllTombstones();
        }

        public void mergeAllDeltas(boolean force) {
            amzaService.mergeAllDeltas(true);
        }

        public void expunge() throws Exception {
            amzaService.expunge();
        }
    }
}
