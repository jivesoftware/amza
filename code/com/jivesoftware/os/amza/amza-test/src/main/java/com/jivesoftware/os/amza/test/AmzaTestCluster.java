package com.jivesoftware.os.amza.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jivesoftware.os.amza.service.AmzaChangeIdPacker;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer;
import com.jivesoftware.os.amza.service.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.amza.service.AmzaTable;
import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TableStorage;
import com.jivesoftware.os.amza.shared.TableStorageProvider;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.storage.FileBackedTableStorage;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.RowTableFile;
import com.jivesoftware.os.amza.storage.json.StringRowMarshaller;
import com.jivesoftware.os.amza.storage.json.StringRowReader;
import com.jivesoftware.os.amza.storage.json.StringRowWriter;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
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
            public <K, V> void sendChangeSet(RingHost ringHost,
                    TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
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
            public <K, V> boolean take(RingHost ringHost,
                    TableName<K, V> mapName,
                    long transationId,
                    TransactionSetStream transactionSetStream) throws Exception {
                AmzaNode service = cluster.get(ringHost);
                if (service == null) {
                    throw new IllegalStateException("Service doesn't exists for " + ringHost);
                } else {
                    service.takeTable(mapName, transationId, transactionSetStream);
                    return true;
                }
            }
        };

        // TODO need to get writer id from somewhere other than port.
        final OrderIdProvider orderIdProvider = new OrderIdProviderImpl(serviceHost.getPort(), new AmzaChangeIdPacker(), new JiveEpochTimestampProvider());

        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);

        TableStorageProvider tableStorageProvider = new TableStorageProvider() {
            @Override
            public <K, V> TableStorage<K, V> createTableStorage(File workingDirectory, String tableDomain, TableName<K, V> tableName) throws Exception {
                File file = new File(workingDirectory, tableDomain + File.separator + tableName.getTableName() + ".kvt");
                StringRowReader reader = new StringRowReader(file);
                StringRowWriter writer = new StringRowWriter(file);

                RowMarshaller<K, V, String> rowMarshaller = new StringRowMarshaller<>(mapper, tableName);
                RowTableFile<K, V, String> rowTableFile = new RowTableFile<>(orderIdProvider, rowMarshaller, reader, writer);
                return new FileBackedTableStorage(rowTableFile);
            }
        };

        AmzaService amzaService = new AmzaServiceInitializer().initialize(config,
                orderIdProvider,
                tableStorageProvider,
                tableStorageProvider,
                tableStorageProvider,
                changeSetSender,
                tableTaker, new TableStateChanges<Object, Object>() {

                    @Override
                    public void changes(TableName<Object, Object> tableName, TableDelta<Object, Object> changes) throws Exception {
                    }
                });

        amzaService.start(serviceHost, config.resendReplicasIntervalInMillis,
                config.applyReplicasIntervalInMillis,
                config.takeFromNeighborsIntervalInMillis,
                config.compactTombstoneIfOlderThanNMillis);

        //if (serviceHost.getPort() % 2 == 0) {
        final TableName tableName = new TableName("test", "table1", String.class, null, null, String.class);
        amzaService.watch(tableName, new TableStateChanges<Object, Object>() {
            @Override
            public void changes(TableName<Object, Object> tableName, TableDelta<Object, Object> changes) throws Exception {
                if (changes.getAppliedRows().size() > 0) {
                    System.out.println("Service:" + serviceHost
                            + " Table:" + tableName.getTableName()
                            + " Changed:" + changes.getAppliedRows().size());
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

        <K, V> void addToReplicatedWAL(TableName<K, V> mapName,
                NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenAdding)) {
                throw new RuntimeException("Random connection failure:" + serviceHost);
            }
            amzaService.receiveChanges(mapName, changes);
        }

        public <K, V> void update(TableName<K, V> tableName, K k, V v, long timestamp, boolean tombstone) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            AmzaTable<K, V> amzaTable = amzaService.getTable(tableName);
            if (tombstone) {
                amzaTable.remove(k);
            } else {
                amzaTable.set(k, v);
            }

        }

        public <K, V> V get(TableName<K, V> tableName, K key) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            AmzaTable<K, V> amzaTable = amzaService.getTable(tableName);
            return amzaTable.get(key);
        }

        public <K, V> void takeTable(TableName<K, V> tableName, long transationId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
            if (off) {
                throw new RuntimeException("Service is off:" + serviceHost);
            }
            if (random.nextInt(100) > (100 - oddsOfAConnectionFailureWhenTaking)) {
                throw new RuntimeException("Random take failure:" + serviceHost);
            }
            AmzaTable<K, V> got = amzaService.getTable(tableName);
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