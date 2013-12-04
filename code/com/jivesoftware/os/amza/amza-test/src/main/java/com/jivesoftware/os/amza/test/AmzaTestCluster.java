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

import com.jivesoftware.os.amza.service.AmzaChangeIdPacker;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.AmzaTable;
import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableIndexProvider;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.storage.FileBackedTableStorage;
import com.jivesoftware.os.amza.storage.FstMarshaller;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.RowTable;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowReader;
import com.jivesoftware.os.amza.storage.binary.BinaryRowWriter;
import com.jivesoftware.os.amza.storage.chunks.Filer;
import com.jivesoftware.os.amza.storage.chunks.IFiler;
import com.jivesoftware.os.amza.storage.index.MapDBTableIndex;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import de.ruedigermoeller.serialization.FSTConfiguration;
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
        config.workingDirectory = workingDirctory.getAbsolutePath() + "/" + serviceHost.getHost() + "-" + serviceHost.getPort();
        config.replicationFactor = 4;
        config.takeFromFactor = 4;
        config.resendReplicasIntervalInMillis = 1000;
        config.applyReplicasIntervalInMillis = 100;
        config.takeFromNeighborsIntervalInMillis = 1000;
        config.compactTombstoneIfOlderThanNMillis = 1000L;

        ChangeSetSender changeSetSender = new ChangeSetSender() {
            @Override
            public void sendChangeSet(RingHost ringHost,
                    TableName mapName, TableIndex changes) throws Exception {
                AmzaNode service = cluster.get(ringHost);
                if (service == null) {
                    throw new IllegalStateException("Service doesn't exists for " + ringHost);
                } else {
                    service.addToReplicatedWAL(mapName, changes);
                }
            }
        };

        ChangeSetTaker tableTaker = new ChangeSetTaker() {

            @Override
            public void take(RingHost ringHost,
                    TableName mapName,
                    long transationId,
                    TransactionSetStream transactionSetStream) throws Exception {
                AmzaNode service = cluster.get(ringHost);
                if (service == null) {
                    throw new IllegalStateException("Service doesn't exists for " + ringHost);
                } else {
                    service.takeTable(mapName, transationId, transactionSetStream);
                }
            }
        };

        // TODO need to get writer id from somewhere other than port.
        final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(serviceHost.getPort(), new AmzaChangeIdPacker(), new JiveEpochTimestampProvider());

        final TableIndexProvider tableIndexProvider = new TableIndexProvider() {

            @Override
            public TableIndex createTableIndex(TableName tableName) {
                return new MapDBTableIndex(tableName.getTableName());
            }
        };

        TableStorageProvider tableStorageProvider = new TableStorageProvider() {
            @Override
            public TableStorage createTableStorage(File workingDirectory, String tableDomain, TableName tableName) throws Exception {
                File file = new File(workingDirectory, tableDomain + File.separator + tableName.getTableName() + ".kvt");
                file.getParentFile().mkdirs();
                IFiler filer = Filer.open(file, "rw");
                BinaryRowReader reader = new BinaryRowReader(filer);
                BinaryRowWriter writer = new BinaryRowWriter(filer);
                RowMarshaller<byte[]> rowMarshaller = new BinaryRowMarshaller();
                RowTable<byte[]> rowTableFile = new RowTable(tableName, orderIdProvider, tableIndexProvider, rowMarshaller, reader, writer);
                return new FileBackedTableStorage(rowTableFile);
            }
        };

        FstMarshaller marshaller = new FstMarshaller(FSTConfiguration.getDefaultConfiguration());
        AmzaService amzaService = new AmzaServiceInitializer().initialize(config,
                orderIdProvider,
                marshaller,
                tableStorageProvider,
                tableStorageProvider,
                tableStorageProvider,
                changeSetSender,
                tableTaker, new TableStateChanges() {

                    @Override
                    public void changes(TableName tableName, TableDelta changes) throws Exception {
                    }
                });

        amzaService.start(serviceHost, config.resendReplicasIntervalInMillis,
                config.applyReplicasIntervalInMillis,
                config.takeFromNeighborsIntervalInMillis,
                config.compactTombstoneIfOlderThanNMillis);

        //if (serviceHost.getPort() % 2 == 0) {
        final TableName tableName = new TableName("test", "table1", null, null);
        amzaService.watch(tableName, new TableStateChanges() {
            @Override
            public void changes(TableName tableName, TableDelta changes) throws Exception {
                if (changes.getApply().size() > 0) {
                    System.out.println("Service:" + serviceHost
                            + " Table:" + tableName.getTableName()
                            + " Changed:" + changes.getApply().size());
                }
            }
        });
        //}

        amzaService.addRingHost("test", serviceHost); // ?? Hacky
        amzaService.addRingHost("MASTER", serviceHost); // ?? Hacky
        if (lastAmzaService != null) {
            amzaService.addRingHost("test", lastAmzaService.ringHost()); // ?? Hacky
            amzaService.addRingHost("MASTER", lastAmzaService.ringHost()); // ?? Hacky

            lastAmzaService.addRingHost("test", serviceHost); // ?? Hacky
            lastAmzaService.addRingHost("MASTER", serviceHost); // ?? Hacky
        }
        lastAmzaService = amzaService;

        service = new AmzaNode(serviceHost, amzaService);
        cluster.put(serviceHost, service);
        System.out.println("Added serviceHost:" + serviceHost + " to the cluster.");
        return service;
    }

    public class AmzaNode {

        private final Random random = new Random();
        private final RingHost serviceHost;
        private final AmzaService amzaService;
        private boolean off = false;
        private int flapped = 0;

        public AmzaNode(RingHost serviceHost, AmzaService amzaService) {
            this.serviceHost = serviceHost;
            this.amzaService = amzaService;
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

        void addToReplicatedWAL(TableName mapName,
                TableIndex changes) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenAdding)) {
                throw new RuntimeException("Random connection failure:" + serviceHost);
            }
            amzaService.receiveChanges(mapName, changes);
        }

        public void update(TableName tableName, TableIndexKey k, byte[] v, long timestamp, boolean tombstone) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            AmzaTable amzaTable = amzaService.getTable(tableName);
            if (tombstone) {
                amzaTable.remove(k);
            } else {
                amzaTable.set(k, v);
            }

        }

        public byte[] get(TableName tableName, TableIndexKey key) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            AmzaTable amzaTable = amzaService.getTable(tableName);
            return amzaTable.get(key);
        }

        public void takeTable(TableName tableName, long transationId, TransactionSetStream transactionSetStream) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenTaking)) {
                throw new RuntimeException("Random take failure:" + serviceHost);
            }
            AmzaTable got = amzaService.getTable(tableName);
            if (got != null) {
                got.getMutatedRowsSince(transationId, transactionSetStream);
            }
        }

        public void printService() throws Exception {
            if (off) {
                System.out.println(serviceHost.getHost() + ":" + serviceHost.getPort() + " is OFF flapped:" + flapped);
                return;
            }
            amzaService.printService();
        }

        public boolean compare(AmzaNode service) throws Exception {
            if (off || service.off) {
                return true;
            }
            Map<TableName, AmzaTable> aTables = amzaService.getTables();
            Map<TableName, AmzaTable> bTables = service.amzaService.getTables();

            Set<TableName> tableNames = new HashSet<>();
            tableNames.addAll(aTables.keySet());
            tableNames.addAll(bTables.keySet());

            for (TableName tableName : tableNames) {
                AmzaTable a = amzaService.getTable(tableName);
                AmzaTable b = service.amzaService.getTable(tableName);
                if (!a.compare(b)) {
                    return false;
                }
            }
            return true;
        }
    }
}
