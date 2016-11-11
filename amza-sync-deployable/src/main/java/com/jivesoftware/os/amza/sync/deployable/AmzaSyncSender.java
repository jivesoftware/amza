package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class AmzaSyncSender {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties PROGRESS_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private static final PartitionProperties CURSOR_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private final AmzaClientAquariumProvider amzaClientAquariumProvider;
    private final int syncRingStripes;
    private final ExecutorService executorService;
    private final int syncThreadCount;
    private final long syncIntervalMillis;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaSyncClient toSyncClient;
    private final ObjectMapper mapper;
    private final Map<PartitionName, PartitionName> whitelist;
    private final int batchSize;
    private final BAInterner interner;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SetMultimap<PartitionName, PartitionName> ensuredPartitions = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public AmzaSyncSender(AmzaClientAquariumProvider amzaClientAquariumProvider,
        int syncRingStripes,
        ExecutorService executorService,
        int syncThreadCount,
        long syncIntervalMillis,
        PartitionClientProvider partitionClientProvider,
        AmzaSyncClient toSyncClient,
        ObjectMapper mapper,
        Map<PartitionName, PartitionName> whitelist,
        int batchSize,
        BAInterner interner) {
        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncThreadCount = syncThreadCount;
        this.syncIntervalMillis = syncIntervalMillis;
        this.partitionClientProvider = partitionClientProvider;
        this.toSyncClient = toSyncClient;
        this.mapper = mapper;
        this.whitelist = whitelist;
        this.batchSize = batchSize;
        this.interner = interner;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < syncThreadCount; i++) {
                int index = i;
                executorService.submit(() -> {
                    while (running.get()) {
                        try {
                            for (int j = 0; j < syncRingStripes; j++) {
                                if (threadIndex(j) == index) {
                                    syncStripe(j);
                                }
                            }
                            Thread.sleep(syncIntervalMillis);
                        } catch (InterruptedException e) {
                            LOG.info("Sync thread {} was interrupted", index);
                        } catch (Throwable t) {
                            LOG.error("Failure in sync thread {}", new Object[] { index }, t);
                            Thread.sleep(syncIntervalMillis);
                        }
                    }
                    return null;
                });
            }

            for (int i = 0; i < syncRingStripes; i++) {
                amzaClientAquariumProvider.register("sync-stripe-" + i);
            }
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            executorService.shutdownNow();
        }
    }

    public interface ProgressStream {
        boolean stream(PartitionName toPartitionName, Cursor cursor) throws Exception;
    }

    public void streamCursors(PartitionName fromPartitionName, PartitionName toPartitionName, ProgressStream stream) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] fromKey = cursorKey(fromPartitionName, toPartitionName);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        cursorClient.scan(Consistency.leader_quorum, true,
            prefixedKeyRangeStream -> {
                return prefixedKeyRangeStream.stream(null, fromKey, null, toKey);
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    PartitionName partitionName = cursorToPartitionName(key);
                    Cursor cursor = mapper.readValue(value, SerializableCursor.class).toCursor();
                    return stream.stream(partitionName, cursor);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    public boolean resetCursors(PartitionName partitionName) throws Exception {
        ensuredPartitions.removeAll(partitionName);

        PartitionClient cursorClient = cursorClient();
        byte[] fromCursorKey = cursorKey(partitionName, null);
        byte[] toCursorKey = WALKey.prefixUpperExclusive(fromCursorKey);
        List<byte[]> cursorKeys = Lists.newArrayList();
        cursorClient.scan(Consistency.leader_quorum, false,
            prefixedKeyRangeStream -> prefixedKeyRangeStream.stream(null, fromCursorKey, null, toCursorKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    cursorKeys.add(key);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        if (!cursorKeys.isEmpty()) {
            cursorClient.commit(Consistency.leader_quorum, null,
                commitKeyValueStream -> {
                    for (byte[] cursorKey : cursorKeys) {
                        commitKeyValueStream.commit(cursorKey, null, -1, true);
                    }
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());
        }

        LOG.info("Reset progress for partition:{} cursors:{} progress:{}", partitionName, cursorKeys.size());
        return true;
    }

    private boolean isElected(int syncStripe) throws Exception {
        LivelyEndState livelyEndState = livelyEndState(syncStripe);
        return livelyEndState != null && livelyEndState.isOnline() && livelyEndState.getCurrentState() == State.leader;
    }

    private LivelyEndState livelyEndState(int syncStripe) throws Exception {
        return amzaClientAquariumProvider.livelyEndState("sync-stripe-" + syncStripe);
    }

    private int threadIndex(int syncStripe) {
        return syncStripe % syncThreadCount;
    }

    private PartitionClient cursorClient() throws Exception {
        return partitionClientProvider.getPartition(cursorName(), 3, CURSOR_PROPERTIES);
    }

    private PartitionName cursorName() {
        byte[] name = ("amza-sync-cursor").getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, name, name);
    }

    private void syncStripe(int stripe) throws Exception {
        if (!isElected(stripe)) {
            return;
        }

        LOG.info("Syncing stripe:{}", stripe);
        int partitionCount = 0;
        int rowCount = 0;
        Map<PartitionName, PartitionName> partitions;
        if (whitelist != null) {
            partitions = whitelist;
        } else {
            partitions = Maps.newHashMap();
            LOG.warn("Syncing all partitions is not supported yet");
            List<PartitionName> allPartitions = Lists.newArrayList(); //TODO partitionClientProvider.getAllPartitionNames();
            for (PartitionName partitionName : allPartitions) {
                partitions.put(partitionName, partitionName);
            }
        }
        for (Entry<PartitionName, PartitionName> entry : partitions.entrySet()) {
            PartitionName fromPartitionName = entry.getKey();
            PartitionName toPartitionName = entry.getValue();
            if (!ensurePartition(fromPartitionName, toPartitionName) || !isElected(stripe)) {
                break;
            }
            int partitionStripe = Math.abs(fromPartitionName.hashCode() % syncRingStripes);
            if (partitionStripe == stripe) {
                partitionCount++;

                int synced = syncPartition(fromPartitionName, toPartitionName, stripe);
                if (synced > 0) {
                    LOG.info("Synced stripe:{} tenantId:{} rows:{}", stripe, fromPartitionName, synced);
                }
                rowCount += synced;
            }
        }
        LOG.info("Synced stripe:{} partitions:{} rows:{}", stripe, partitionCount, rowCount);
    }

    private boolean ensurePartition(PartitionName fromPartitionName, PartitionName toPartitionName) throws Exception {
        if (!ensuredPartitions.containsEntry(fromPartitionName, toPartitionName)) {
            PartitionProperties properties = partitionClientProvider.getProperties(fromPartitionName);
            if (properties == null) {
                return false;
            }

            LOG.info("Submitting properties fromPartitionName:{} toPartitionName:{}", fromPartitionName, toPartitionName);
            toSyncClient.ensurePartition(toPartitionName, properties);
            ensuredPartitions.put(fromPartitionName, toPartitionName);
        }
        return true;
    }

    private int syncPartition(PartitionName fromPartitionName, PartitionName toPartitionName, int stripe) throws Exception {
        Cursor cursor = getPartitionCursor(fromPartitionName, toPartitionName);
        PartitionClient fromClient = partitionClientProvider.getPartition(fromPartitionName);
        if (!isElected(stripe)) {
            return 0;
        }

        int synced = 0;
        while (true) {
            List<Row> rows = Lists.newArrayListWithCapacity(batchSize);
            TakeResult takeResult = fromClient.takeFromTransactionId(null,
                cursor,
                highwater -> {
                    for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                        cursor.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                    }
                },
                (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    rows.add(new Row(prefix, key, value, valueTimestamp, valueTombstoned));
                    return rows.size() < batchSize;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());

            if (!isElected(stripe)) {
                return synced;
            }
            if (!rows.isEmpty()) {
                toSyncClient.commitRows(toPartitionName, rows);
                synced += rows.size();
            }

            cursor.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);
            if (takeResult.tookToEnd != null) {
                for (WALHighwater.RingMemberHighwater ringMemberHighwater : takeResult.tookToEnd.ringMemberHighwater) {
                    cursor.merge(ringMemberHighwater.ringMember, ringMemberHighwater.transactionId, Math::max);
                }
            }

            savePartitionCursor(fromPartitionName, toPartitionName, cursor);
            if (takeResult.tookToEnd != null) {
                break;
            }
        }

        return synced;
    }

    private Cursor getPartitionCursor(PartitionName fromPartitionName, PartitionName toPartitionName) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromPartitionName, toPartitionName);
        SerializableCursor[] result = new SerializableCursor[1];
        cursorClient.get(Consistency.leader_quorum, null,
            unprefixedWALKeyStream -> unprefixedWALKeyStream.stream(cursorKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    result[0] = mapper.readValue(value, SerializableCursor.class);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        return result[0] != null ? result[0].toCursor() : new Cursor();
    }

    private void savePartitionCursor(PartitionName fromPartitionName, PartitionName toPartitionName, Cursor cursor) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromPartitionName, toPartitionName);
        byte[] value = mapper.writeValueAsBytes(SerializableCursor.fromCursor(cursor));
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(cursorKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    private byte[] cursorKey(PartitionName fromPartitionName, PartitionName toPartitionName) {
        if (toPartitionName == null) {
            byte[] fromBytes = fromPartitionName.toBytes();
            byte[] key = new byte[2 + fromBytes.length];
            UIO.unsignedShortBytes(fromBytes.length, key, 0);
            UIO.writeBytes(fromBytes, key, 2);
            return key;
        } else {
            byte[] fromBytes = fromPartitionName.toBytes();
            byte[] toBytes = toPartitionName.toBytes();
            byte[] key = new byte[2 + fromBytes.length + 2 + toBytes.length];
            UIO.unsignedShortBytes(fromBytes.length, key, 0);
            UIO.writeBytes(fromBytes, key, 2);
            UIO.unsignedShortBytes(toBytes.length, key, 2 + fromBytes.length);
            UIO.writeBytes(toBytes, key, 2 + fromBytes.length + 2);
            return key;
        }
    }

    private PartitionName cursorToPartitionName(byte[] key) {
        int fromPartitionLength = UIO.bytesUnsignedShort(key, 0);
        int toPartitionLength = UIO.bytesUnsignedShort(key, 2 + fromPartitionLength);
        byte[] toPartitionBytes = new byte[toPartitionLength];
        UIO.readBytes(key, 2 + fromPartitionLength + 2, toPartitionBytes);
        return PartitionName.fromBytes(toPartitionBytes, 0, interner);
    }

    private static class SerializableCursor extends HashMap<String, Long> {

        public static SerializableCursor fromCursor(Cursor cursor) {
            SerializableCursor serializableCursor = new SerializableCursor();
            for (Entry<RingMember, Long> entry : cursor.entrySet()) {
                serializableCursor.put(entry.getKey().getMember(), entry.getValue());
            }
            return serializableCursor;
        }

        public Cursor toCursor() {
            Cursor cursor = new Cursor();
            for (Entry<String, Long> entry : entrySet()) {
                cursor.put(new RingMember(entry.getKey()), entry.getValue());
            }
            return cursor;
        }
    }
}
