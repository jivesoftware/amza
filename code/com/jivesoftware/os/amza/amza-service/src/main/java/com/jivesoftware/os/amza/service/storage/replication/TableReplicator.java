package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.service.storage.TableStore;
import com.jivesoftware.os.amza.service.storage.TableStoreProvider;
import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.HighWaterMarks;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
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
        for (Entry<TableName, TableStore<?, ?>> table : tables.getTableStores()) {
            TableStore<?, ?> value = table.getValue();
            try {
                value.compactTombestone(ifOlderThanNMillis);
            } catch (Exception x) {
                LOG.warn("Failed to compact tombstones table:" + table.getKey(), x);
            }
        }
    }

    public void takeChanges(HostRingProvider hostRingProvider) throws Exception {
        for (Entry<TableName, TableStore<?, ?>> table : tables.getTableStores()) {
            TableName tableName = table.getKey();
            HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
            LOG.debug("Taking changes for " + tableName);
            takeChanges(hostRing.getAboveRing(), table.getKey());
        }
    }

    private <K, V> void takeChanges(RingHost[] ringHosts, TableName<K, V> tableName) throws Exception {
        final MutableInt taken = new MutableInt(0);
        int i = 0;
        final MutableInt leaps = new MutableInt(0);
        TableStore<K, V> tableStore = tables.getTableStore(tableName);
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
                        new TakeTransactionSetStream<>(tableStore, ringHost, tableName, highWaterMarks));

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

    static class TakeTransactionSetStream<K, V> implements TransactionSetStream<K, V> {

        private final TableStore<K, V> tableStore;
        private final RingHost ringHost;
        private final TableName<K, V> tableName;
        private final HighWaterMarks highWaterMarks;

        public TakeTransactionSetStream(TableStore<K, V> tableStore,
            RingHost ringHost,
            TableName<K, V> tableName,
            HighWaterMarks highWaterMarks) {
            this.tableStore = tableStore;
            this.ringHost = ringHost;
            this.tableName = tableName;
            this.highWaterMarks = highWaterMarks;
        }

        @Override
        public boolean stream(TransactionSet<K, V> took) throws Exception {
            if (took != null) {
                NavigableMap<K, TimestampedValue<V>> changes = took.getChanges();
                if (!changes.isEmpty()) {
                    tableStore.commit(changes);
                    highWaterMarks.set(ringHost, tableName, took.getHighestTransactionId());
                }
            }
            return true;
        }
    }

    public <K, V> void receiveChanges(TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        if (!changes.isEmpty()) {
            receivedChangesWAL.getTableStore(mapName).commit(changes);
        }
    }

    synchronized public void applyReceivedChanges() throws Exception {
        for (Map.Entry<TableName, TableStore<?, ?>> replicatedUpdates : receivedChangesWAL.getTableStores()) {
            TableName mapName = replicatedUpdates.getKey();
            TableStore<?, ?> store = replicatedUpdates.getValue();
            NavigableMap<?, TimestampedValue<?>> immutableRows = (NavigableMap<?, TimestampedValue<?>>) store.getImmutableRows();
            if (!immutableRows.isEmpty()) {
                tables.getTableStore(mapName).commit(store.getImmutableRows());
                store.clearAllRows();
            }
        }
    }

    public <K, V> boolean replicateLocalChanges(HostRingProvider hostRingProvider, TableName<K, V> tableName,
        NavigableMap<K, TimestampedValue<V>> changes,
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
                        LOG.debug("Failed to send changeset to ringHost:" + ringHost, x);
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
        for (Map.Entry<TableName, TableStore<?, ?>> table : tables.getTableStores()) {
            resendWAL.getTableStore(table.getKey());
        }

        for (Map.Entry<TableName, TableStore<?, ?>> updates : resendWAL.getTableStores()) {
            TableName tableName = updates.getKey();
            HostRing hostRing = hostRingProvider.getHostRing(tableName.getRingName());
            RingHost[] ring = hostRing.getBelowRing();
            if (ring.length > 0) {
                TableName mapName = updates.getKey();
                TableStore<?, ?> store = updates.getValue();
                if (replicateLocalChanges(hostRingProvider, mapName, store.getImmutableRows(), false)) {
                    store.clearAllRows();
                }
            } else {
                LOG.warn("Trying to resend to an empty ring. tableName:" + tableName);
            }
        }
    }
}
