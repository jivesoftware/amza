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

import com.jivesoftware.os.amza.service.storage.TableStore;
import com.jivesoftware.os.amza.service.storage.TableStoreProvider;
import com.jivesoftware.os.amza.service.storage.TableTransaction;
import com.jivesoftware.os.amza.service.storage.replication.HostRing;
import com.jivesoftware.os.amza.service.storage.replication.HostRingBuilder;
import com.jivesoftware.os.amza.service.storage.replication.HostRingProvider;
import com.jivesoftware.os.amza.service.storage.replication.TableReplicator;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.Marshaller;
import com.jivesoftware.os.amza.shared.MemoryTableIndex;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.EntryStream;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableStateChanges;
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
    private final Marshaller marshaller;
    private final TableReplicator tableReplicator;
    private final AmzaTableWatcher amzaTableWatcher;
    private final TableStoreProvider tableStoreProvider;
    private final TableName tableIndexKey = new TableName("MASTER", "TABLE_INDEX", null, null);

    public AmzaService(OrderIdProvider orderIdProvider,
            Marshaller marshaller,
            TableReplicator tableReplicator,
            TableStoreProvider tableStoreProvider,
            AmzaTableWatcher amzaTableWatcher) {
        this.orderIdProvider = orderIdProvider;
        this.marshaller = marshaller;
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
        byte[] rawRingHost = marshaller.serialize(ringHost);
        TableName ringIndexKey = createRingTableName(ringName);
        TableStore ringIndex = tableStoreProvider.getTableStore(ringIndexKey);
        TableTransaction tx = ringIndex.startTransaction(orderIdProvider.nextId());
        tx.add(new TableIndexKey(rawRingHost), rawRingHost);
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
        byte[] rawRingHost = marshaller.serialize(ringHost);
        TableName ringIndexKey = createRingTableName(ringName);
        TableStore ringIndex = tableStoreProvider.getTableStore(ringIndexKey);
        TableTransaction tx = ringIndex.startTransaction(orderIdProvider.nextId());
        tx.remove(new TableIndexKey(rawRingHost));
        tx.commit();
    }

    @Override
    public List<RingHost> getRing(String ringName) throws Exception {
        TableName ringIndexKey = createRingTableName(ringName);
        TableStore ringIndex = tableStoreProvider.getTableStore(ringIndexKey);
        if (ringIndex == null) {
            LOG.warn("No ring defined for ringName:" + ringName);
            return new ArrayList<>();
        } else {
            final Set<RingHost> ringHosts = new HashSet<>();
            ringIndex.getImmutableRows().entrySet(new EntryStream<Exception>() {

                @Override
                public boolean stream(TableIndexKey key, BinaryTimestampedValue value) throws Exception {
                    if (!value.getTombstoned()) {
                        ringHosts.add(marshaller.deserialize(value.getValue(), RingHost.class));
                    }
                    return true;
                }
            });
            return new ArrayList<>(ringHosts);
        }
    }

    @Override
    public HostRing getHostRing(String ringName) throws Exception {
        return new HostRingBuilder().build(ringHost, getRing(ringName));
    }

    private TableName createRingTableName(String ringName) {
        ringName = ringName.toUpperCase();
        return new TableName("MASTER", "RING_INDEX_" + ringName, null, null);
    }

    private boolean createTable(TableName tableName) throws Exception {
        byte[] rawTableName = marshaller.serialize(tableName);

        TableStore tableNameIndex = tableStoreProvider.getTableStore(tableIndexKey);
        BinaryTimestampedValue timestamptedTableKey = tableNameIndex.getTimestampedValue(new TableIndexKey(rawTableName));
        if (timestamptedTableKey == null) {
            TableTransaction tx = tableNameIndex.startTransaction(orderIdProvider.nextId());
            tx.add(new TableIndexKey(rawTableName), rawTableName);
            tx.commit();
            return true;
        } else {
            return !timestamptedTableKey.getTombstoned();
        }
    }

    public AmzaTable getTable(TableName tableName) throws Exception {
        byte[] rawTableName = marshaller.serialize(tableName);
        TableStore tableStoreIndex = tableStoreProvider.getTableStore(tableIndexKey);
        BinaryTimestampedValue timestampedKeyValueStoreName = tableStoreIndex.getTimestampedValue(new TableIndexKey(rawTableName));
        while (timestampedKeyValueStoreName == null) {
            createTable(tableName);
            timestampedKeyValueStoreName = tableStoreIndex.getTimestampedValue(new TableIndexKey(rawTableName));
        }
        if (timestampedKeyValueStoreName.getTombstoned()) {
            return null;
        } else {
            TableStore tableStore = tableStoreProvider.getTableStore(tableName);
            return new AmzaTable(orderIdProvider, tableName, tableStore);
        }
    }

    @Override
    public List<TableName> getTableNames() {
        List<TableName> amzaTableNames = new ArrayList<>();
        for (Entry<TableName, TableStore> tableStore : tableStoreProvider.getTableStores()) {
            amzaTableNames.add(tableStore.getKey());
        }
        return amzaTableNames;
    }

    public Map<TableName, AmzaTable> getTables() throws Exception {
        Map<TableName, AmzaTable> amzaTables = new HashMap<>();
        for (Entry<TableName, TableStore> tableStore : tableStoreProvider.getTableStores()) {
            amzaTables.put(tableStore.getKey(), new AmzaTable(orderIdProvider, tableStore.getKey(), tableStore.getValue()));
        }
        return amzaTables;
    }

    @Override
    public void destroyTable(TableName tableName) throws Exception {
        byte[] rawTableName = marshaller.serialize(tableName);
        TableStore tableIndex = tableStoreProvider.getTableStore(tableIndexKey);
        TableTransaction tx = tableIndex.startTransaction(orderIdProvider.nextId());
        tx.remove(new TableIndexKey(rawTableName));
        tx.commit();
    }

    public void receiveChanges(TableName mapName, TableIndex changes) throws Exception {
        tableReplicator.receiveChanges(mapName, changes);
    }

    public void watch(TableName tableName, TableStateChanges tableStateChanges) throws Exception {
        amzaTableWatcher.watch(tableName, tableStateChanges);
    }

    public TableStateChanges unwatch(TableName tableName) throws Exception {
        return amzaTableWatcher.unwatch(tableName);
    }

    @Override
    public void changes(TableName tableName, TableDelta changes) throws Exception {
        receiveChanges(tableName, new MemoryTableIndex(changes.getApply()));
    }

    @Override
    public void takeTableChanges(TableName tableName,
            long transationId, TransactionSetStream transactionSetStream) throws Exception {
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
        for (Map.Entry<TableName, TableStore> table : tableStoreProvider.getTableStores()) {
            TableStore sortedMapStore = table.getValue();
            NavigableMap<?, BinaryTimestampedValue> immutableRows = (NavigableMap<?, BinaryTimestampedValue>) sortedMapStore.getImmutableRows();
            for (Map.Entry<?, BinaryTimestampedValue> e : (Set<Map.Entry<?, BinaryTimestampedValue>>) immutableRows.entrySet()) {

                System.out.println(ringHost.getHost() + ":" + ringHost.getPort()
                        + ":" + table.getKey().getTableName() + " k:" + e.getKey() + " v:" + e.getValue().getValue()
                        + " d:" + e.getValue().getTombstoned() + " t:" + e.getValue().getTimestamp());
            }
        }
    }
}
