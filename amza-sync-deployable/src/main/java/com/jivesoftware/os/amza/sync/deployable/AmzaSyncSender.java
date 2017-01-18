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
import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream.TxResult;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionConfig;
import com.jivesoftware.os.amza.sync.api.AmzaSyncPartitionTuple;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class AmzaSyncSender {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties CURSOR_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private final AmzaSyncSenderConfig config;
    private final AmzaClientAquariumProvider amzaClientAquariumProvider;
    private final int syncRingStripes;
    private final ScheduledExecutorService executorService;
    private final ScheduledFuture[] syncFutures;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaSyncClient toSyncClient;
    private final ObjectMapper mapper;
    private final AmzaSyncPartitionConfigProvider syncPartitionConfigProvider;
    private final BAInterner interner;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SetMultimap<PartitionName, PartitionName> ensuredPartitions = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public AmzaSyncSender(AmzaSyncSenderConfig config,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        int syncRingStripes,
        ScheduledExecutorService executorService,
        PartitionClientProvider partitionClientProvider,
        AmzaSyncClient toSyncClient,
        ObjectMapper mapper,
        AmzaSyncPartitionConfigProvider syncPartitionConfigProvider,
        BAInterner interner) {

        this.config = config;
        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncFutures = new ScheduledFuture[syncRingStripes];
        this.partitionClientProvider = partitionClientProvider;
        this.toSyncClient = toSyncClient;
        this.mapper = mapper;
        this.syncPartitionConfigProvider = syncPartitionConfigProvider;
        this.interner = interner;
    }

    public AmzaSyncSenderConfig getConfig() {
        return config;
    }

    public boolean configHasChanged(AmzaSyncSenderConfig senderConfig) {
        return !config.equals(senderConfig);
    }

    private String aquariumName(int syncStripe) {
        return "amza-sync-" + config.name + "-stripe-" + syncStripe;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            for (int i = 0; i < syncRingStripes; i++) {
                amzaClientAquariumProvider.register(aquariumName(i));
            }

            for (int i = 0; i < syncRingStripes; i++) {
                int index = i;
                syncFutures[i] = executorService.scheduleWithFixedDelay(() -> {
                    try {
                        syncStripe(index);
                    } catch (InterruptedException e) {
                        LOG.info("Sync thread {} was interrupted", index);
                    } catch (Throwable t) {
                        LOG.error("Failure in sync thread {}", new Object[] { index }, t);
                    }
                }, 0, config.syncIntervalMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            for (int i = 0; i < syncRingStripes; i++) {
                syncFutures[i].cancel(true);
            }
        }
    }

    public interface ProgressStream {
        boolean stream(PartitionName fromPartitionName, PartitionName toPartitionName, long timestamp, Cursor cursor) throws Exception;
    }

    public void streamCursors(PartitionName fromPartitionName, PartitionName toPartitionName, ProgressStream stream) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] fromKey = fromPartitionName == null ? null : cursorKey(fromPartitionName, toPartitionName);
        byte[] toKey = fromPartitionName == null ? null : WALKey.prefixUpperExclusive(fromKey);
        cursorClient.scan(Consistency.leader_quorum, true,
            prefixedKeyRangeStream -> {
                return prefixedKeyRangeStream.stream(null, fromKey, null, toKey);
            },
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    PartitionName from = cursorKeyFromPartitionName(key);
                    PartitionName to = cursorKeyToPartitionName(key);
                    Cursor cursor = cursorFromValue(value, interner);
                    return stream.stream(from, to, timestamp, cursor);
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
        return amzaClientAquariumProvider.livelyEndState(aquariumName(syncStripe));
    }

    private PartitionClient cursorClient() throws Exception {
        return partitionClientProvider.getPartition(cursorName(), 3, CURSOR_PROPERTIES);
    }

    private PartitionName cursorName() {
        byte[] nameBytes = ("amza-sync-cursor-v2-" + config.name).getBytes(StandardCharsets.UTF_8);
        return new PartitionName(false, nameBytes, nameBytes);
    }

    private void syncStripe(int stripe) throws Exception {
        if (!isElected(stripe)) {
            return;
        }

        LOG.info("Syncing stripe:{}", stripe);
        int partitionCount = 0;
        int rowCount = 0;
        Map<AmzaSyncPartitionTuple, AmzaSyncPartitionConfig> partitions;
        if (syncPartitionConfigProvider != null) {
            partitions = syncPartitionConfigProvider.getAll(config.name);
        } else {
            partitions = Maps.newHashMap();
            LOG.warn("Syncing all partitions is not supported yet");
            List<PartitionName> allPartitions = Lists.newArrayList(); //TODO partitionClientProvider.getAllPartitionNames();
            for (PartitionName partitionName : allPartitions) {
                partitions.put(new AmzaSyncPartitionTuple(partitionName, partitionName), new AmzaSyncPartitionConfig());
            }
        }
        for (Entry<AmzaSyncPartitionTuple, AmzaSyncPartitionConfig> entry : partitions.entrySet()) {
            if (!isElected(stripe)) {
                break;
            }
            PartitionName fromPartitionName = entry.getKey().from;
            PartitionName toPartitionName = entry.getKey().to;
            int partitionStripe = Math.abs(fromPartitionName.hashCode() % syncRingStripes);
            if (partitionStripe == stripe) {
                if (!ensurePartition(fromPartitionName, toPartitionName) || !isElected(stripe)) {
                    break;
                }
                partitionCount++;

                int synced = syncPartition(entry.getKey(), entry.getValue(), stripe);
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
            RingPartitionProperties properties = partitionClientProvider.getProperties(fromPartitionName);
            if (properties == null) {
                LOG.warn("Missing properties fromPartitionName:{}", fromPartitionName);
                return false;
            }

            int ringSize = properties.ringSize;
            if (ringSize <= 0) {
                LOG.warn("Found invalid ringSize:{} fromPartitionName:{}", ringSize, fromPartitionName);
                return false;
            }

            LOG.info("Submitting properties fromPartitionName:{} toPartitionName:{} ringSize:{}", fromPartitionName, toPartitionName, ringSize);
            toSyncClient.ensurePartition(toPartitionName, properties.partitionProperties, ringSize);
            ensuredPartitions.put(fromPartitionName, toPartitionName);
        }
        return true;
    }

    private int syncPartition(AmzaSyncPartitionTuple partitionTuple, AmzaSyncPartitionConfig toPartitionConfig, int stripe) throws Exception {
        PartitionName toPartitionName = partitionTuple.to;
        Cursor cursor = getPartitionCursor(partitionTuple.from, toPartitionName);
        PartitionClient fromClient = partitionClientProvider.getPartition(partitionTuple.from);
        if (!isElected(stripe)) {
            return 0;
        }

        int synced = 0;
        boolean taking = true;
        cursor.taking.set(true);
        while (taking) {
            List<Row> rows = Lists.newArrayListWithExpectedSize(config.batchSize);
            TakeResult takeResult = fromClient.takeFromTransactionId(null,
                cursor.memberTxIds,
                config.batchSize,
                highwater -> {
                    for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                        cursor.memberTxIds.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                    }
                },
                (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    rows.add(new Row(prefix, key, value, valueTimestamp, valueTombstoned));
                    return TxResult.MORE;
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

            cursor.memberTxIds.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);
            if (takeResult.tookToEnd != null) {
                for (WALHighwater.RingMemberHighwater ringMemberHighwater : takeResult.tookToEnd.ringMemberHighwater) {
                    cursor.memberTxIds.merge(ringMemberHighwater.ringMember, ringMemberHighwater.transactionId, Math::max);
                }
                taking = false;
                cursor.taking.set(false);
            }

            savePartitionCursor(partitionTuple.from, toPartitionName, cursor);
        }

        return synced;
    }

    private Cursor getPartitionCursor(PartitionName fromPartitionName, PartitionName toPartitionName) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromPartitionName, toPartitionName);
        Cursor[] result = new Cursor[1];
        cursorClient.get(Consistency.leader_quorum, null,
            unprefixedWALKeyStream -> unprefixedWALKeyStream.stream(cursorKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    result[0] = cursorFromValue(value, interner);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        return result[0] != null ? result[0] : new Cursor(true, Maps.newHashMap());
    }

    private void savePartitionCursor(PartitionName fromPartitionName, PartitionName toPartitionName, Cursor cursor) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromPartitionName, toPartitionName);
        byte[] value = valueFromCursor(cursor);
        cursorClient.commit(Consistency.leader_quorum, null,
            commitKeyValueStream -> commitKeyValueStream.commit(cursorKey, value, -1, false),
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    private static byte[] valueFromCursor(Cursor cursor) {
        int valueLength = 1 + 1 + 2;
        for (RingMember ringMember : cursor.memberTxIds.keySet()) {
            valueLength += 2 + ringMember.sizeInBytes() + 8;
        }

        byte[] value = new byte[1 + 1 + 2 + valueLength];
        value[0] = 1; // version
        value[1] = (byte) (cursor.taking.get() ? 1 : 0);
        UIO.unsignedShortBytes(cursor.memberTxIds.size(), value, 2);
        int o = 4;
        for (Entry<RingMember, Long> entry : cursor.memberTxIds.entrySet()) {
            int memberLength = entry.getKey().sizeInBytes();
            UIO.unsignedShortBytes(memberLength, value, o);
            o += 2;
            entry.getKey().toBytes(value, o);
            o += memberLength;
            UIO.longBytes(entry.getValue(), value, o);
            o += 8;
        }
        return value;
    }

    private static Cursor cursorFromValue(byte[] value, BAInterner interner) throws InterruptedException {
        if (value[0] == 1) {
            boolean taking = value[1] == 1;

            int memberTxIdsLength = UIO.bytesUnsignedShort(value, 2);
            int o = 4;

            Map<RingMember, Long> memberTxIds = Maps.newHashMap();
            for (int i = 0; i < memberTxIdsLength; i++) {
                int memberLength = UIO.bytesUnsignedShort(value, o);
                o += 2;
                RingMember member = RingMember.fromBytes(value, o, memberLength, interner);
                o += memberLength;
                long txId = UIO.bytesLong(value, o);
                memberTxIds.put(member, txId);
                o += 8;
            }

            return new Cursor(taking, memberTxIds);
        } else {
            LOG.error("Unsupported cursor version {}", value[0]);
            return null;
        }
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

    private PartitionName cursorKeyFromPartitionName(byte[] key) throws InterruptedException {
        int fromPartitionLength = UIO.bytesUnsignedShort(key, 0);
        byte[] fromPartitionBytes = new byte[fromPartitionLength];
        UIO.readBytes(key, 2, fromPartitionBytes);
        return PartitionName.fromBytes(fromPartitionBytes, 0, interner);
    }

    private PartitionName cursorKeyToPartitionName(byte[] key) throws InterruptedException {
        int fromPartitionLength = UIO.bytesUnsignedShort(key, 0);
        int toPartitionLength = UIO.bytesUnsignedShort(key, 2 + fromPartitionLength);
        byte[] toPartitionBytes = new byte[toPartitionLength];
        UIO.readBytes(key, 2 + fromPartitionLength + 2, toPartitionBytes);
        return PartitionName.fromBytes(toPartitionBytes, 0, interner);
    }
}
