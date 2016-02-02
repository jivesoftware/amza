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

import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.take.TakeCursors;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.AmzaTestCluster.AmzaNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AmzaServiceTest {

    @Test(enabled = true)
    public void testAddToReplicatedWAL() throws Exception {
        final int maxNumberOfServices = 5;

        // TODO test this using all version
        //String indexClassType = BerkeleyDBWALIndexProvider.INDEX_CLASS_NAME;
        //String indexClassType = LSMPointerIndexWALIndexProvider.INDEX_CLASS_NAME;
        String indexClassType = "memory_persistent";

        File createTempDir = Files.createTempDir();
        AmzaTestCluster cluster = new AmzaTestCluster(createTempDir, 0, 0);
        for (int i = 0; i < maxNumberOfServices; i++) {
            cluster.newNode(new RingMember("localhost-" + i), new RingHost("datacenter", "rack", "localhost", i));
        }
        Collection<AmzaNode> clusterNodes = cluster.getAllNodes();

        testRowType(cluster,
            maxNumberOfServices,
            new PartitionName(false, "test".getBytes(), "partition1".getBytes()),
            indexClassType,
            RowType.primary,
            Consistency.quorum,
            Consistency.quorum);

        testRowType(cluster,
            maxNumberOfServices,
            new PartitionName(false, "test".getBytes(), "partition2".getBytes()),
            indexClassType,
            RowType.snappy_primary,
            Consistency.quorum, Consistency.quorum);

        System.out.println("\n------ensuring non empty---------");
        for (AmzaNode a : clusterNodes) {
            Assert.assertFalse(a.isEmpty());
        }

        assertConsistency(Consistency.quorum, clusterNodes);

        System.out.println("\n------force tombstone compaction---------");
        for (AmzaNode a : clusterNodes) {
            a.compactAllTombstones();
        }
        assertConsistency(Consistency.quorum, clusterNodes);

        System.out.println("\n------force delta merge---------");
        for (AmzaNode a : clusterNodes) {
            a.mergeAllDeltas(true);
        }
        assertConsistency(Consistency.quorum, clusterNodes);

        System.out.println("\n------force expunge---------");
        for (AmzaNode a : clusterNodes) {
            a.expunge();
        }
        assertConsistency(Consistency.quorum, clusterNodes);

        System.out.println("\n------stopping---------");
        for (AmzaNode a : clusterNodes) {
            a.stop();
        }

        System.out.println("-------------------------");
        System.out.println("-------------------------");
        System.out.println("-------------------------");
        System.out.println("---- Restarting :) ------");
        System.out.println("-------------------------");
        System.out.println("-------------------------");
        System.out.println("-------------------------");

        cluster = new AmzaTestCluster(createTempDir, 0, 0);
        for (AmzaNode a : clusterNodes) {
            cluster.newNode(a.ringMember, a.ringHost);
        }

        testRowType(cluster,
            maxNumberOfServices,
            new PartitionName(false, "test".getBytes(), "partition1".getBytes()),
            indexClassType,
            RowType.primary,
            Consistency.quorum,
            Consistency.quorum);

        testRowType(cluster, maxNumberOfServices,
            new PartitionName(false, "test".getBytes(), "partition2".getBytes()),
            indexClassType,
            RowType.snappy_primary,
            Consistency.quorum,
            Consistency.quorum);

        clusterNodes = cluster.getAllNodes();
        for (AmzaNode a : clusterNodes) {
            Assert.assertFalse(a.isEmpty());
        }

        assertConsistency(Consistency.quorum, clusterNodes);
        System.out.println("\n------stopping---------");
        for (AmzaNode a : clusterNodes) {
            a.stop();
        }

        System.out.println("-------------------------");
        System.out.println("-------------------------");
        System.out.println("-------------------------");
        System.out.println("------PASSED :) ---------");
        System.out.println("-------------------------");
        System.out.println("-------------------------");
        System.out.println("-------------------------");
    }

    private void assertConsistency(Consistency readConsistency, Collection<AmzaNode> clusterNodes) throws Exception {
        int falseCount = -1;
        while (falseCount != 0) {
            falseCount = 0;
            System.out.println("---- checking for inconsistencies ----");
            List<AmzaNode> nodes = new ArrayList<>(clusterNodes);
            DONE:
            for (int i = 0; i < nodes.size(); i++) {
                AmzaNode a = nodes.get(i);
                for (int j = 0; j < nodes.size(); j++) {
                    if (i == j) {
                        continue;
                    }
                    AmzaNode b = nodes.get(j);
                    if (!a.compare(readConsistency, b)) {
                        System.out.println(a + " is NOT consistent with " + b);
                        falseCount++;
                        break DONE;
                    }
                    System.out.println(a + " is consistent with " + b);
                }
            }
            if (falseCount > 0) {
                Thread.sleep(1000);
                System.out.println("---------------------------------------------------------------------\n\n\n\n");
            }
        }
    }

    static class Took implements TxKeyValueStream {

        private final Map<WALKey, TookValue> took = new ConcurrentHashMap<>();

        @Override
        public boolean stream(long rowTxId, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) throws
            Exception {
            TookValue tookValue = new TookValue(rowTxId, value, valueTimestamp, valueTombstoned, valueVersion);
            took.compute(new WALKey(prefix, key), (WALKey k, TookValue existing) -> {
                if (existing == null) {
                    return tookValue;
                } else {
                    return existing.compareTo(tookValue) >= 0 ? existing : tookValue;
                }
            });
            return true;
        }

        static class TookValue implements Comparable<TookValue> {

            private final long rowTxId;
            private final byte[] value;
            private final long valueTimestamp;
            private final boolean valueTombstoned;
            private final long valueVersion;

            public TookValue(long rowTxId, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) {
                this.rowTxId = rowTxId;
                this.value = value;
                this.valueTimestamp = valueTimestamp;
                this.valueTombstoned = valueTombstoned;
                this.valueVersion = valueVersion;
            }

            @Override
            public int compareTo(TookValue o) {
                int c = Long.compare(valueTimestamp, o.valueTimestamp);
                if (c != 0) {
                    return c;
                }
                c = Long.compare(valueVersion, o.valueVersion);
                return c;
            }
        }

    }

    private void testRowType(AmzaTestCluster cluster,
        int maxNumberOfServices,
        PartitionName partitionName,
        String indexClassType,
        RowType rowType,
        Consistency readConsistency,
        Consistency writeConsistency
    ) throws Exception {
        final int maxUpdates = 100;
        final int delayBetweenUpdates = 0;
        final int maxFields = 10;
        final int maxOffServices = 0;
        final int maxRemovedServices = 0;
        final int maxAddService = 0;

        final Random random = new Random();
        AtomicBoolean updating = new AtomicBoolean(true);

        ExecutorService takerThreadPool = Executors.newFixedThreadPool(maxNumberOfServices + maxAddService);
        List<Future> takerFutures = new ArrayList<>();
        Took[] took = new Took[maxNumberOfServices];
        for (int i = 0; i < maxNumberOfServices; i++) {
            int serviceId = i;
            took[i] = new Took();
            takerThreadPool.submit(() -> {
                RingMember ringMember = new RingMember("localhost-" + serviceId);
                AmzaNode node = cluster.get(ringMember);
                boolean tookToEnd = false;
                long txId = -1;
                while (!tookToEnd || updating.get()) {
                    try {
                        Thread.sleep(100);
                        TakeCursors takeCursors = node.takeFromTransactionId(partitionName, txId, took[serviceId]);
                        tookToEnd = takeCursors.tookToEnd;
                        for (TakeCursors.RingMemberCursor ringMemberCursor : takeCursors.ringMemberCursors) {
                            if (ringMemberCursor.ringMember.equals(ringMember)) {
                                txId = ringMemberCursor.transactionId;
                            }
                        }
                    } catch (FailedToAchieveQuorumException x) {
                        System.out.println("Waiting for system ready...");
                    } catch (InterruptedException x) {
                        Thread.interrupted();
                        break;
                    } catch (IllegalStateException | PropertiesNotPresentException x) {
                        // Swallow for now.
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            });
        }

        ExecutorService updateThreadPool = Executors.newFixedThreadPool(maxNumberOfServices + maxAddService);
        List<Future> updatesFutures = new ArrayList<>();
        for (int i = 0; i < maxUpdates; i++) {
            updatesFutures.add(updateThreadPool.submit(new Callable<Void>() {
                int removeService = maxRemovedServices;
                int addService = maxAddService;
                int offService = maxOffServices;

                @Override
                public Void call() throws Exception {
                    AmzaNode node = cluster.get(new RingMember("localhost-" + random.nextInt(maxNumberOfServices)));
                    if (node != null) {
                        node.create(writeConsistency, partitionName, indexClassType, rowType);
                        boolean tombstone = random.nextBoolean();
                        String prefix = "a";
                        String key = String.valueOf(random.nextInt(maxFields));
                        byte[] indexPrefix = prefix.getBytes();
                        byte[] indexKey = key.getBytes();
                        while (true) {
                            try {
                                node.update(writeConsistency, partitionName, indexPrefix, indexKey, ("" + random.nextInt()).getBytes(), tombstone);
                                Thread.sleep(delayBetweenUpdates);
                                node.get(readConsistency, partitionName, indexPrefix, indexKey);
                                break;
                            } catch (DeltaOverCapacityException x) {
                                System.out.println("Delta over capacity, waiting...");
                                Thread.sleep(100);
                            }
                        }
                    }

                    if (removeService > 0) {
                        RingMember key = new RingMember("localhost-" + random.nextInt(maxNumberOfServices));
                        node = cluster.get(key);
                        try {
                            if (node != null) {
                                System.out.println("Removing node:" + key);
                                cluster.remove(key);
                                node.stop();
                                removeService--;
                            }
                        } catch (Exception x) {
                            System.out.println("Failed to remove node: " + node);
                            x.printStackTrace();
                        }
                    }

                    if (addService > 0) {
                        int port = maxNumberOfServices + random.nextInt(maxAddService);
                        RingMember key = new RingMember("localhost-" + port);
                        node = cluster.get(key);
                        try {
                            if (node == null) {
                                cluster.newNode(new RingMember("localhost-" + port), new RingHost("datacenter", "rack", "localhost", port));
                                addService--;
                            }
                        } catch (Exception x) {
                            System.out.println("Failed to add node: " + key);
                            x.printStackTrace();
                        }
                    }

                    if (offService > 0) {
                        RingMember key = new RingMember("localhost-" + random.nextInt(maxNumberOfServices));
                        node = cluster.get(key);
                        if (node != null) {
                            try {
                                node.setOff(!node.isOff());
                                offService--;
                            } catch (Exception x) {
                                System.out.println("Issues while turning node off:" + x.getMessage());
                            }
                        }

                    }

                    return null;
                }
            }));
        }

        for (Future future : updatesFutures) {
            future.get();
        }
        updating.set(false);
        for (Future future : takerFutures) {
            future.get();
        }

        Collection<AmzaNode> clusterNodes = cluster.getAllNodes();
        for (AmzaNode node : clusterNodes) {
            AmzaService.AmzaPartitionRoute route = node.getPartitionRoute(partitionName);
            assertEquals(route.orderedMembers.size(), clusterNodes.size());
            assertNotNull(route.leader);
            assertTrue(node.sickThreads.getSickThread().isEmpty());
            assertTrue(node.sickPartitions.getSickPartitions().isEmpty());
        }

        takerThreadPool.shutdownNow();
        updateThreadPool.shutdownNow();
    }

}
