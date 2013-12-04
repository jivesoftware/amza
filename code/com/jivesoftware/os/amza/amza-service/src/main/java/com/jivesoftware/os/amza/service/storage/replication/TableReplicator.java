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
package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.service.storage.TableStore;
import com.jivesoftware.os.amza.service.storage.TableStoreProvider;
import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.HighWaterMarks;
import com.jivesoftware.os.amza.shared.MemoryTableIndex;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableInt;

/**
 *
 */
public class TableReplicator {

    /**
     * Host initiating change: 1. Ordered list of hosts. 2. Identify self in list. 3. Send change to hosts 1 above and 1 below A. on failure 1. add to
     * guaranteed delivery. 2. Continually try to a host above until successful or you have looped all the way around to your self.
     *
     * Host receiving change: 1. Apply change: A. No Op - end of call. B. Change applied 1. Treat change as an initiating change
     *
     */
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TableStoreProvider tables;
    private final int replicationFactor;
    private final int takeFromFactor;
    private final TableStoreProvider receivedChangesWAL;
    private final TableStoreProvider resendWAL;
    private final ChangeSetSender changeSetSender;
    private final ChangeSetTaker tableTaker;
    private final HighWaterMarks highWaterMarks;

    public TableReplicator(TableStoreProvider tables,
            int replicationFactor,
            int takeFromFactor,
            HighWaterMarks highWaterMarks,
            TableStoreProvider replicatedWAL,
            TableStoreProvider resendWAL,
            ChangeSetSender changeSetSender,
            ChangeSetTaker tableTaker) {
        this.tables = tables;
        this.replicationFactor = replicationFactor;
        this.takeFromFactor = takeFromFactor;
        this.highWaterMarks = highWaterMarks;
        this.receivedChangesWAL = replicatedWAL;
        this.resendWAL = resendWAL;
        this.changeSetSender = changeSetSender;
        this.tableTaker = tableTaker;
    }

    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        for (Entry<TableName, TableStore> table : tables.getTableStores()) {
            TableStore value = table.getValue();
            try {
                value.compactTombestone(ifOlderThanNMillis);
            } catch (Exception x) {
                LOG.warn("Failed to compact tombstones table:" + table.getKey(), x);
            }
        }
    }

    public void takeChanges(HostRingProvider hostRingProvider) throws Exception {
        for (Entry<TableName, TableStore> table : tables.getTableStores()) {
            TableName tableName = table.getKey();
            HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
            LOG.debug("Taking changes for " + tableName);
            takeChanges(hostRing.getAboveRing(), table.getKey());
        }
    }

    private void takeChanges(RingHost[] ringHosts, TableName tableName) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        TableStore tableStore = tables.getTableStore(tableName);
        while (i < ringHosts.length) {
            i = (leaps.intValue() * 2);
            for (; i < ringHosts.length; i++) {
                RingHost ringHost = ringHosts[i];
                if (ringHost == null) {
                    continue;
                }
                ringHosts[i] = null;
                try {
                    Long lastTransactionId = highWaterMarks.get(ringHost, tableName);
                    LOG.debug("Taking from " + ringHost + " " + tableName + " " + lastTransactionId);
                    tableTaker.take(ringHost, tableName, lastTransactionId,
                            new TakeTransactionSetStream(tableStore, ringHost, tableName, highWaterMarks));

                    taken.increment();
                    if (taken.intValue() >= takeFromFactor) {
                        return;
                    }
                    leaps.increment();
                    break;

                } catch (Exception x) {
                    LOG.debug("Can't takeFrom host:" + ringHost, x);
                    LOG.warn("Can't takeFrom host:" + ringHost);
                }
            }
        }
    }

    static class TakeTransactionSetStream implements TransactionSetStream {

        private final TableStore tableStore;
        private final RingHost ringHost;
        private final TableName tableName;
        private final HighWaterMarks highWaterMarks;

        public TakeTransactionSetStream(TableStore tableStore,
                RingHost ringHost,
                TableName tableName,
                HighWaterMarks highWaterMarks) {
            this.tableStore = tableStore;
            this.ringHost = ringHost;
            this.tableName = tableName;
            this.highWaterMarks = highWaterMarks;
        }

        @Override
        public boolean stream(TransactionSet took) throws Exception {
            if (took != null) {
                NavigableMap<TableIndexKey, TimestampedValue> changes = took.getChanges();
                if (!changes.isEmpty()) {
                    tableStore.commit(new MemoryTableIndex(changes));
                    highWaterMarks.set(ringHost, tableName, took.getHighestTransactionId());
                }
            }
            return true;
        }
    }

    public void receiveChanges(TableName mapName, TableIndex changes) throws Exception {
        if (!changes.isEmpty()) {
            receivedChangesWAL.getTableStore(mapName).commit(changes);
        }
    }

    synchronized public void applyReceivedChanges() throws Exception {
        for (Map.Entry<TableName, TableStore> replicatedUpdates : receivedChangesWAL.getTableStores()) {
            TableName mapName = replicatedUpdates.getKey();
            TableStore store = replicatedUpdates.getValue();
            TableIndex immutableRows = store.getImmutableRows();
            if (!immutableRows.isEmpty()) {
                tables.getTableStore(mapName).commit(store.getImmutableRows());
                store.clearAllRows();
            }
        }
    }

    public boolean replicateLocalChanges(HostRingProvider hostRingProvider, TableName tableName,
            TableIndex changes,
            boolean enqueueForResendOnFailure) throws Exception {
        if (changes.isEmpty()) {
            return true;
        } else {
            HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
            RingHost[] ringHosts = hostRing.getBelowRing();
            if (ringHosts == null || ringHosts.length == 0) {
                if (enqueueForResendOnFailure) {
                    resendWAL.getTableStore(tableName).commit(changes);
                }
                return false;
            } else {
                RingWalker ringWalker = new RingWalker(ringHosts, replicationFactor);
                RingHost ringHost;
                while ((ringHost = ringWalker.host()) != null) {
                    try {
                        changeSetSender.sendChangeSet(ringHost, tableName, changes);
                        ringWalker.success();
                    } catch (Exception x) {
                        ringWalker.failed();
                        LOG.info("Failed to send changeset to ringHost:" + ringHost, x);
                        LOG.warn("Failed to send changeset to ringHost:" + ringHost);
                        if (enqueueForResendOnFailure) {
                            resendWAL.getTableStore(tableName).commit(changes);
                            enqueueForResendOnFailure = false;
                        }
                    }
                }
                return ringWalker.wasAdequetlyReplicated();
            }
        }
    }

    public void resendLocalChanges(HostRingProvider hostRingProvider) throws Exception {

        // Hacky
        for (Map.Entry<TableName, TableStore> table : tables.getTableStores()) {
            resendWAL.getTableStore(table.getKey());
        }

        for (Map.Entry<TableName, TableStore> updates : resendWAL.getTableStores()) {
            TableName tableName = updates.getKey();
            HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
            RingHost[] ring = hostRing.getBelowRing();
            if (ring.length > 0) {
                TableName mapName = updates.getKey();
                TableStore store = updates.getValue();
                // TODO this copy sucks
                final MemoryTableIndex memoryTableIndex = new MemoryTableIndex(new TreeMap<TableIndexKey, TimestampedValue>());
                store.getImmutableRows().entrySet(new TableIndex.EntryStream<Exception>() {

                    @Override
                    public boolean stream(TableIndexKey key, TimestampedValue value) throws Exception {
                        memoryTableIndex.put(key, value);
                        return true;
                    }
                });
                // END TODO this copy sucks
                if (replicateLocalChanges(hostRingProvider, mapName, memoryTableIndex, false)) {
                    store.clearAllRows();
                }
            } else {
                LOG.warn("Trying to resend to an empty ring. tableName:" + tableName);
            }
        }
    }
}
