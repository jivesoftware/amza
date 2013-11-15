package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.service.storage.TableStore;
import com.jivesoftware.os.amza.service.storage.TableStoreProvider;
import com.jivesoftware.os.amza.service.storage.TableTransaction;
import com.jivesoftware.os.amza.service.storage.replication.HostRing;
import com.jivesoftware.os.amza.service.storage.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.replication.HostRingProvider;
import com.jivesoftware.os.amza.service.storage.replication.TableReplicator;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * Amza pronounced (AH m z ah )
 *
 * Sanskrit word meaning partition / share.
 *
 *
 */
public class AmzaService implements HostRingProvider, AmzaInstance {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private RingHost ringHost;
    private ScheduledExecutorService scheduledThreadPool;
    private final OrderIdProvider orderIdProvider;
    private final TableReplicator tableReplicator;
    private final AmzaTableWatcher amzaTableWatcher;
    private final TableStoreProvider tableStoreProvider;
    private final TableName tableIndexKey = new TableName<>("MASTER", "TABLE_INDEX",
            TableName.class, null, null, TableName.class);

    public AmzaService(OrderIdProvider orderIdProvider,
            TableReplicator tableReplicator,
            TableStoreProvider tableStoreProvider,
            AmzaTableWatcher amzaTableWatcher) {
        this.orderIdProvider = orderIdProvider;
        this.tableReplicator = tableReplicator;
        this.tableStoreProvider = tableStoreProvider;
        this.amzaTableWatcher = amzaTableWatcher;
    }

    synchronized public RingHost ringHost() {
        return ringHost;
    }

    synchronized public void start(RingHost ringHost, long resendReplicasIntervalInMillis,
            long applyReplicasIntervalInMillis,
            long takeFromNeighborsIntervalInMillis,
            final long compactTombstoneIfOlderThanNMillis) throws Exception {
        if (scheduledThreadPool == null) {
            this.ringHost = ringHost;
            scheduledThreadPool = Executors.newScheduledThreadPool(4);
            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        tableReplicator.resendLocalChanges(AmzaService.this);
                    } catch (Exception x) {
                        LOG.debug("Failed while resending replicas.", x);
                        LOG.error("Failed while resending replicas.", x);
                    }
                }
            }, resendReplicasIntervalInMillis, resendReplicasIntervalInMillis, TimeUnit.MILLISECONDS);

            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        tableReplicator.applyReceivedChanges();
                    } catch (Exception x) {
                        LOG.debug("Failing to replay apply replication.", x);
                        LOG.error("Failing to replay apply replication.", x);
                    }
                }
            }, applyReplicasIntervalInMillis, applyReplicasIntervalInMillis, TimeUnit.MILLISECONDS);

            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        tableReplicator.takeChanges(AmzaService.this);
                    } catch (Exception x) {
                        LOG.debug("Failing to take from above and below.", x);
                        LOG.error("Failing to take from above and below.");
                    }
                }
            }, takeFromNeighborsIntervalInMillis, takeFromNeighborsIntervalInMillis, TimeUnit.MILLISECONDS);

            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        tableReplicator.compactTombestone(compactTombstoneIfOlderThanNMillis);
                    } catch (Exception x) {
                        LOG.debug("Failing to compact tombestones.", x);
                        LOG.error("Failing to compact tombestones.");
                    }
                }
            }, compactTombstoneIfOlderThanNMillis, compactTombstoneIfOlderThanNMillis, TimeUnit.MILLISECONDS);

            tableReplicator.takeChanges(AmzaService.this);
        }
    }

    synchronized public void stop() throws Exception {
        this.ringHost = null;
        if (scheduledThreadPool != null) {
            this.scheduledThreadPool.shutdownNow();
            this.scheduledThreadPool = null;
        }
    }

    @Override
    public void addRingHost(String ringName, RingHost ringHost) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        if (ringHost == null) {
            throw new IllegalArgumentException("ringHost cannot be null.");
        }
        TableName ringIndexKey = createRingTableName(ringName);
        TableStore<RingHost, RingHost> ringIndex = tableStoreProvider.getTableStore(ringIndexKey);
        TableTransaction<RingHost, RingHost> tx = ringIndex.startTransaction(orderIdProvider.nextId());
        tx.add(ringHost, ringHost);
        tx.commit();
    }

    @Override
    public void removeRingHost(String ringName, RingHost ringHost) throws Exception {
        if (ringName == null) {
            throw new IllegalArgumentException("ringName cannot be null.");
        }
        if (ringHost == null) {
            throw new IllegalArgumentException("ringHost cannot be null.");
        }
        TableName ringIndexKey = createRingTableName(ringName);
        TableStore<RingHost, RingHost> ringIndex = tableStoreProvider.getTableStore(ringIndexKey);
        TableTransaction<RingHost, RingHost> tx = ringIndex.startTransaction(orderIdProvider.nextId());
        tx.remove(ringHost);
        tx.commit();
    }

    @Override
    public List<RingHost> getRing(String ringName) throws Exception {
        TableName ringIndexKey = createRingTableName(ringName);
        TableStore<RingHost, RingHost> ringIndex = tableStoreProvider.getTableStore(ringIndexKey);
        if (ringIndex == null) {
            LOG.warn("No ring defined for ringName:" + ringName);
            return new ArrayList<>();
        } else {
            Set<RingHost> ringHosts = new HashSet<>();
            for (TimestampedValue<RingHost> value : ringIndex.getImmutableRows().values()) {
                if (!value.getTombstoned()) {
                    ringHosts.add(value.getValue());
                }
            }
            return new ArrayList<>(ringHosts);
        }
    }

    @Override
    public HostRing getHostRing(String ringName) throws Exception {
        return new HostRingBuilder().build(ringHost, getRing(ringName));
    }

    private TableName<RingHost, RingHost> createRingTableName(String ringName) {
        ringName = ringName.toUpperCase();
        return new TableName<>("MASTER", "RING_INDEX_" + ringName,
                RingHost.class, null, null, RingHost.class);
    }

    private <K, V> boolean createTable(TableName<K, V> tableName) throws Exception {

        TableStore<TableName, TableName> tableNameIndex = tableStoreProvider.getTableStore(tableIndexKey);
        TimestampedValue<TableName> timestamptedTableKey = tableNameIndex.getTimestampedValue(tableName);
        if (timestamptedTableKey == null) {
            TableTransaction<TableName, TableName> tx = tableNameIndex.startTransaction(orderIdProvider.nextId());
            tx.add(tableName, tableName);
            tx.commit();
            return true;
        } else {
            return !timestamptedTableKey.getTombstoned();
        }
    }

    public <K, V> AmzaTable<K, V> getTable(TableName<K, V> tableName) throws Exception {
        TableStore<TableName, TableName> tableStoreIndex = tableStoreProvider.getTableStore(tableIndexKey);
        TimestampedValue<TableName> timestampedKeyValueStoreName = tableStoreIndex.getTimestampedValue(tableName);
        while (timestampedKeyValueStoreName == null) {
            createTable(tableName);
            timestampedKeyValueStoreName = tableStoreIndex.getTimestampedValue(tableName);
        }
        if (timestampedKeyValueStoreName.getTombstoned()) {
            return null;
        } else {
            TableStore<K, V> tableStore = tableStoreProvider.getTableStore(tableName);
            return new AmzaTable<>(orderIdProvider, tableName, tableStore);
        }
    }

    public Map<TableName, AmzaTable> getTables() throws Exception {
        Map<TableName, AmzaTable> amzaTables = new HashMap<>();
        for (Entry<TableName, TableStore<?, ?>> tableStore : tableStoreProvider.getTableStores()) {
            amzaTables.put(tableStore.getKey(), new AmzaTable(orderIdProvider, tableStore.getKey(), tableStore.getValue()));
        }
        return amzaTables;
    }

    public <K, V> void destroyTable(TableName<K, V> tableName) throws Exception {
        TableStore<TableName, TableName> tableIndex = tableStoreProvider.getTableStore(tableIndexKey);
        TableTransaction<TableName, TableName> tx = tableIndex.startTransaction(orderIdProvider.nextId());
        tx.remove(tableName);
        tx.commit();
    }

    public <K, V> void receiveChanges(TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        tableReplicator.receiveChanges(mapName, changes);
    }

    public <K, V> void watch(TableName<K, V> tableName, TableStateChanges tableStateChanges) throws Exception {
        amzaTableWatcher.watch(tableName, tableStateChanges);
    }

    public <K, V> TableStateChanges unwatch(TableName<K, V> tableName) throws Exception {
        return amzaTableWatcher.unwatch(tableName);
    }

    @Override
    public <K, V> void changes(TableName<K, V> tableName, TableDelta<K, V> changes) throws Exception {
        receiveChanges(tableName, changes.getApply());
    }

    @Override
    public <K, V> void takeTableChanges(TableName<K, V> tableName,
            long transationId, TransactionSetStream<K, V> transactionSetStream) throws Exception {
        getTable(tableName).getMutatedRowsSince(transationId, transactionSetStream);
    }

    public void buildRandomSubRing(String ringName, int desiredRingSize) throws Exception {
        List<RingHost> ring = getRing("MASTER");
        if (ring.size() < desiredRingSize) {
            throw new IllegalStateException("Current master ring is not large enough to support a ring of size:" + desiredRingSize);
        }
        Collections.shuffle(ring);
        for (int i = 0; i < desiredRingSize; i++) {
            addRingHost(ringName, ring.get(i));
        }
    }

    //------ Used for debugging ------
    public void printService() throws Exception {
        for (Map.Entry<TableName, TableStore<?, ?>> table : tableStoreProvider.getTableStores()) {
            TableStore<?, ?> sortedMapStore = table.getValue();
            NavigableMap<?, TimestampedValue<?>> immutableRows = (NavigableMap<?, TimestampedValue<?>>) sortedMapStore.getImmutableRows();
            for (Map.Entry<?, TimestampedValue<?>> e : (Set<Map.Entry<?, TimestampedValue<?>>>) immutableRows.entrySet()) {

                System.out.println(ringHost.getHost() + ":" + ringHost.getPort()
                        + ":" + table.getKey().getTableName() + " k:" + e.getKey() + " v:" + e.getValue().getValue()
                        + " d:" + e.getValue().getTombstoned() + " t:" + e.getValue().getTimestamp());
            }
        }
    }

}