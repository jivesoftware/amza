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

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.test.AmzaTestCluster.AmzaNode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class AmzaServiceTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        FileUtils.deleteDirectory(new File("./tmp"));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        FileUtils.deleteDirectory(new File("./tmp"));
    }

    @Test(enabled = true)
    public void testAddToReplicatedWAL() throws Exception {
        final int maxUpdates = 100;
        final int delayBetweenUpdates = 0;
        final int maxFields = 10;
        final int maxOffServices = 0;
        final int maxRemovedServices = 0;
        final int maxAddService = 0;
        final int maxNumberOfServices = 5;

        final Random random = new Random();

        final AmzaTestCluster cluster = new AmzaTestCluster(new File("./tmp"), 0, 0);

        for (int i = 0; i < maxNumberOfServices; i++) {
            cluster.newNode(new RingHost("localhost", i));
        }
        final TableName mapName = new TableName("test", "table1", null, null);
        final CountDownLatch latch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            int removeService = maxRemovedServices;
            int addService = maxAddService;
            int offService = maxOffServices;

            @Override
            public void run() {
                for (int i = 0; i < maxUpdates; i++) {
                    try {
                        AmzaNode node = cluster.get(new RingHost("localhost", random.nextInt(maxNumberOfServices)));
                        if (node != null) {
                            boolean tombstone = random.nextBoolean();
                            String key = "a-" + random.nextInt(maxFields);
                            TableIndexKey indexKey = new TableIndexKey(key.getBytes());
                            node.update(mapName, indexKey, ("" + random.nextInt()).getBytes(), System.currentTimeMillis(), tombstone);
                            Thread.sleep(delayBetweenUpdates);
                            node.get(mapName, indexKey);
                        }
                    } catch (Exception x) {
                        System.out.println(x.getMessage());
                    }

                    try {
                        if (removeService > 0) {
                            RingHost key = new RingHost("localhost", random.nextInt(maxNumberOfServices));
                            AmzaNode node = cluster.get(key);
                            if (node != null) {
                                System.out.println("Removing node:" + key);
                                cluster.remove(key);
                                node.stop();
                                removeService--;
                            }
                        }
                    } catch (Exception x) {
                        System.out.println(x.getMessage());
                    }

                    try {
                        if (addService > 0) {
                            int port = maxNumberOfServices + random.nextInt(maxAddService);
                            RingHost key = new RingHost("localhost", port);
                            AmzaNode node = cluster.get(key);
                            if (node == null) {
                                cluster.newNode(new RingHost("localhost", port));
                                addService--;
                            }
                        }
                    } catch (Exception x) {
                        System.out.println(x.getMessage());
                    }

                    try {
                        if (offService > 0) {
                            RingHost key = new RingHost("localhost", random.nextInt(maxNumberOfServices));
                            AmzaNode node = cluster.get(key);
                            if (node != null) {
                                node.setOff(!node.isOff());
                                offService--;
                            }
                        }
                    } catch (Exception x) {
                        System.out.println(x.getMessage());
                    }

                }
                latch.countDown();
            }
        });

        latch.await();

        int falseCount = -1;
        while (falseCount != 0) {
            falseCount = 0;
            System.out.println("---- checking for inconsistencies ----");
            List<AmzaNode> nodes = new ArrayList<>(cluster.getAllNodes());
            DONE:
            for (int i = 0; i < nodes.size(); i++) {
                AmzaNode a = nodes.get(i);
                for (int j = i + 1; j < nodes.size(); j++) {
                    AmzaNode b = nodes.get(j);
                    if (!a.compare(b)) {
                        falseCount++;
                        break DONE;
                    }
                    System.out.println(a + " is consistent with " + b);
                }
            }
            if (falseCount > 0) {
                Thread.sleep(10000);
            }
        }
        System.out.println("\n------stopping---------");
        for (AmzaNode a : cluster.getAllNodes()) {
            a.stop();
        }
        System.out.println("\n------PASSED :) ---------");

//        System.out.println("\n------final-state---------");
//        for (Service a : cluster.values()) {
//            a.printService();
//        }
    }

}
