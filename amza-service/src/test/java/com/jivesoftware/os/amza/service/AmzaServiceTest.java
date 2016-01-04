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
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.AmzaTestCluster.AmzaNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import junit.framework.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class AmzaServiceTest {

    @Test(enabled = true)
    public void testAddToReplicatedWAL() throws Exception {
        final int maxNumberOfServices = 5;

        File createTempDir = Files.createTempDir();
        AmzaTestCluster cluster = new AmzaTestCluster(createTempDir, 0, 0);
        for (int i = 0; i < maxNumberOfServices; i++) {
            cluster.newNode(new RingMember("localhost-" + i), new RingHost("localhost", i));
        }
        Collection<AmzaNode> clusterNodes = cluster.getAllNodes();

        testRowType(cluster, maxNumberOfServices, new PartitionName(false, "test".getBytes(), "partition1".getBytes()), RowType.primary, Consistency.quorum,
            Consistency.quorum);
        testRowType(cluster, maxNumberOfServices, new PartitionName(false, "test".getBytes(), "partition2".getBytes()), RowType.snappy_primary,
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

        System.out.println("\n------restarting---------");
        cluster = new AmzaTestCluster(createTempDir, 0, 0);
        for (AmzaNode a : clusterNodes) {
            cluster.newNode(a.ringMember, a.ringHost);
        }

        testRowType(cluster, maxNumberOfServices, new PartitionName(false, "test".getBytes(), "partition1".getBytes()), RowType.primary, Consistency.quorum,
            Consistency.quorum);
        testRowType(cluster, maxNumberOfServices, new PartitionName(false, "test".getBytes(), "partition2".getBytes()), RowType.snappy_primary,
            Consistency.quorum, Consistency.quorum);

        clusterNodes = cluster.getAllNodes();
        for (AmzaNode a : clusterNodes) {
            Assert.assertFalse(a.isEmpty());
        }

        assertConsistency(Consistency.quorum, clusterNodes);
        System.out.println("\n------stopping---------");
        for (AmzaNode a : clusterNodes) {
            a.stop();
        }

        System.out.println("\n------PASSED :) ---------");
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

    private void testRowType(AmzaTestCluster cluster, int maxNumberOfServices, PartitionName partitionName, RowType rowType,
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

        ExecutorService threadPool = Executors.newCachedThreadPool();
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < maxUpdates; i++) {
            futures.add(threadPool.submit(new Runnable() {
                int removeService = maxRemovedServices;
                int addService = maxAddService;
                int offService = maxOffServices;

                @Override
                public void run() {
                    AmzaNode node = cluster.get(new RingMember("localhost-" + random.nextInt(maxNumberOfServices)));
                    try {
                        if (node != null) {
                            node.create(writeConsistency, partitionName, rowType);
                            boolean tombstone = random.nextBoolean();
                            String prefix = "a";
                            String key = String.valueOf(random.nextInt(maxFields));
                            byte[] indexPrefix = prefix.getBytes();
                            byte[] indexKey = key.getBytes();
                            node.update(writeConsistency, partitionName, indexPrefix, indexKey, ("" + random.nextInt()).getBytes(), tombstone);
                            Thread.sleep(delayBetweenUpdates);
                            node.get(readConsistency, partitionName, indexPrefix, indexKey);
                        }
                    } catch (Exception x) {
                        System.out.println("Failed to update node: " + node);
                        x.printStackTrace();
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
                                cluster.newNode(new RingMember("localhost-" + port), new RingHost("localhost", port));
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
                        try {
                            if (node != null) {
                                node.setOff(!node.isOff());
                                offService--;
                            }
                        } catch (Exception x) {
                            System.out.println(x.getMessage());
                        }
                    }
                }
            }));
        }

        for (Future future : futures) {
            future.get();
        }

        Collection<AmzaNode> clusterNodes = cluster.getAllNodes();
        for (AmzaNode node : clusterNodes) {
            AmzaService.AmzaPartitionRoute route = node.getPartitionRoute(partitionName);
            assertEquals(route.orderedMembers.size(), clusterNodes.size());
            assertNotNull(route.leader);
        }
    }

}
