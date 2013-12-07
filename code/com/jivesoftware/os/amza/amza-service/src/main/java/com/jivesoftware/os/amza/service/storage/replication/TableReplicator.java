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
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.MemoryRowsIndex;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

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
    private final UpdatesSender updatesSender;
    private final UpdatesTaker updatesTaker;
    private final HighwaterMarks highwaterMarks;

    public TableReplicator(TableStoreProvider tables,
            int replicationFactor,
            int takeFromFactor,
            HighwaterMarks highwaterMarks,
            TableStoreProvider replicatedWAL,
            TableStoreProvider resendWAL,
            UpdatesSender updatesSender,
            UpdatesTaker updatesTaker) {
        this.tables = tables;
        this.replicationFactor = replicationFactor;
        this.takeFromFactor = takeFromFactor;
        this.highwaterMarks = highwaterMarks;
        this.receivedChangesWAL = replicatedWAL;
        this.resendWAL = resendWAL;
        this.updatesSender = updatesSender;
        this.updatesTaker = updatesTaker;
    }

    public void compactTombestone(long ifOlderThanNMillis) throws Exception {
        for (Entry<TableName, TableStore> table : tables.getTableStores()) {
            TableStore tableStore = table.getValue();
            try {
                tableStore.compactTombestone(ifOlderThanNMillis);
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
            takeChanges(hostRing.getAboveRing(), tableName);
        }
    }

    private void takeChanges(RingHost[] ringHosts, TableName tableName) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        TableStore tableStore = tables.getRowsStore(tableName);
        while (i < ringHosts.length) {
            i = (leaps.intValue() * 2);
            for (; i < ringHosts.length; i++) {
                RingHost ringHost = ringHosts[i];
                if (ringHost == null) {
                    continue;
                }
                ringHosts[i] = null;
                try {
                    Long highwaterMark = highwaterMarks.get(ringHost, tableName);
                    LOG.trace("Taking from " + ringHost + " " + tableName + " " + highwaterMark);
                    updatesTaker.takeUpdates(ringHost, tableName, highwaterMark,
                            new TakeRowStream(tableStore, ringHost, tableName, highwaterMarks));

                    TakeRowStream takeRowStream = new TakeRowStream(tableStore, ringHost, tableName, highwaterMarks);
                    updatesTaker.takeUpdates(ringHost, tableName, highwaterMark, takeRowStream);
                    takeRowStream.flush();

                    taken.increment();
                    if (taken.intValue() >= takeFromFactor) {
                        return;
                    }
                    leaps.increment();
                    break;

                } catch (Exception x) {
                    LOG.debug("Can't takeFrom host:" + ringHost, x);
                    LOG.warn("Can't takeFrom host:" + ringHost + " " + x.getMessage());
                }
            }
        }
    }

    // TODO fix known issues around how highwater marks are handled
    static class TakeRowStream implements RowScan {

        private final TableStore tableStore;
        private final RingHost ringHost;
        private final TableName tableName;
        private final HighwaterMarks highWaterMarks;
        private final MutableLong highWaterMark;
        private final TreeMap<RowIndexKey, RowIndexValue> batch = new TreeMap<>();

        public TakeRowStream(TableStore tableStore,
                RingHost ringHost,
                TableName tableName,
                HighwaterMarks highWaterMarks) {
            this.tableStore = tableStore;
            this.ringHost = ringHost;
            this.tableName = tableName;
            this.highWaterMarks = highWaterMarks;
            this.highWaterMark = new MutableLong(highWaterMarks.get(ringHost, tableName));
        }

        @Override
        public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws Exception {
            if (highWaterMark.longValue() < orderId) {
                highWaterMark.setValue(orderId);
            }
            batch.put(key, value);
            if (batch.size() > 1) {
                flush();
            }
            return true;
        }

        public void flush() throws Exception {
            if (!batch.isEmpty()) {
                System.out.println("Took:" + batch.size() + " from " + ringHost + " for " + tableName);
                tableStore.commit(new MemoryRowsIndex(batch));
                highWaterMarks.set(ringHost, tableName, highWaterMark.longValue());
                batch.clear();
            }
            if (highWaterMark.longValue() < 0) {
                highWaterMarks.set(ringHost, tableName, 0);
            }
        }
    }

    public void receiveChanges(TableName mapName, RowScanable changes) throws Exception {
        receivedChangesWAL.getRowsStore(mapName).commit(changes);
    }

    // TODO move synchronized to a tighter scope
    synchronized public void applyReceivedChanges() throws Exception {
        for (Map.Entry<TableName, TableStore> replicatedUpdates : receivedChangesWAL.getTableStores()) {
            TableName mapName = replicatedUpdates.getKey();
            TableStore tableStore = tables.getRowsStore(mapName);
            TableStore updates = replicatedUpdates.getValue();
            tableStore.commit(updates);
            updates.clear();
        }
    }

    public boolean replicateLocalUpdates(HostRingProvider hostRingProvider,
            TableName tableName,
            RowScanable rowUpdates,
            boolean enqueueForResendOnFailure) throws Exception {

        HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
        RingHost[] ringHosts = hostRing.getBelowRing();
        if (ringHosts == null || ringHosts.length == 0) {
            if (enqueueForResendOnFailure) {
                resendWAL.getRowsStore(tableName).commit(rowUpdates);
            }
            return false;
        } else {
            RingWalker ringWalker = new RingWalker(ringHosts, replicationFactor);
            RingHost ringHost;
            while ((ringHost = ringWalker.host()) != null) {
                try {
                    updatesSender.sendUpdates(ringHost, tableName, rowUpdates);
                    ringWalker.success();
                } catch (Exception x) {
                    ringWalker.failed();
                    LOG.info("Failed to send changeset to ringHost:" + ringHost, x);
                    LOG.warn("Failed to send changeset to ringHost:" + ringHost);
                    if (enqueueForResendOnFailure) {
                        resendWAL.getRowsStore(tableName).commit(rowUpdates);
                        enqueueForResendOnFailure = false;
                    }
                }
            }
            return ringWalker.wasAdequetlyReplicated();
        }
    }

    public void resendLocalChanges(HostRingProvider hostRingProvider) throws Exception {

        // TODO eval why this is Hacky
        for (Map.Entry<TableName, TableStore> table : tables.getTableStores()) {
            resendWAL.getRowsStore(table.getKey());
        }

        for (Map.Entry<TableName, TableStore> updates : resendWAL.getTableStores()) {
            TableName tableName = updates.getKey();
            HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
            RingHost[] ring = hostRing.getBelowRing();
            if (ring.length > 0) {
                TableName mapName = updates.getKey();
                TableStore store = updates.getValue();
                if (replicateLocalUpdates(hostRingProvider, mapName, store, false)) {
                    store.clear(); // TODO this is a BUG cause anything that came along while we were replicating is lost.
                }
            } else {
                LOG.warn("Trying to resend to an empty ring. tableName:" + tableName);
            }
        }
    }
}
