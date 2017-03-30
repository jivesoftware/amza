package com.jivesoftware.os.amza.sync.deployable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.jivesoftware.os.amza.api.AmzaInterner;
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class AmzaSyncSender {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties CURSOR_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, 0, 0, 0, 0,
        false, Consistency.leader_quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private static final Cursor DEFAULT_CURSOR = new Cursor(true, true, -1, -1, Maps.newHashMap());

    private final AmzaSyncStats stats;
    private final AmzaSyncSenderConfig config;
    private final AmzaClientAquariumProvider amzaClientAquariumProvider;
    private final int syncRingStripes;
    private final ScheduledExecutorService executorService;
    private final ScheduledFuture[] syncFutures;
    private final PartitionClientProvider partitionClientProvider;
    private final AmzaSyncClient toSyncClient;
    private final AmzaSyncPartitionConfigProvider syncPartitionConfigProvider;
    private final AmzaInterner amzaInterner;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SetMultimap<PartitionName, PartitionName> ensuredPartitions = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    private final long additionalSolverAfterNMillis = 10_000; //TODO expose to conf?
    private final long abandonLeaderSolutionAfterNMillis = 30_000; //TODO expose to conf?
    private final long abandonSolutionAfterNMillis = 60_000; //TODO expose to conf?

    public AmzaSyncSender(AmzaSyncStats stats,
        AmzaSyncSenderConfig config,
        AmzaClientAquariumProvider amzaClientAquariumProvider,
        int syncRingStripes,
        ScheduledExecutorService executorService,
        PartitionClientProvider partitionClientProvider,
        AmzaSyncClient toSyncClient,
        AmzaSyncPartitionConfigProvider syncPartitionConfigProvider,
        AmzaInterner amzaInterner) {
        this.stats = stats;

        this.config = config;
        this.amzaClientAquariumProvider = amzaClientAquariumProvider;
        this.syncRingStripes = syncRingStripes;
        this.executorService = executorService;
        this.syncFutures = new ScheduledFuture[syncRingStripes];
        this.partitionClientProvider = partitionClientProvider;
        this.toSyncClient = toSyncClient;
        this.syncPartitionConfigProvider = syncPartitionConfigProvider;
        this.amzaInterner = amzaInterner;
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
                    Cursor cursor = cursorFromValue(value);
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
                partitions.put(new AmzaSyncPartitionTuple(partitionName, partitionName), new AmzaSyncPartitionConfig(-1, -1, -1, -1, 0)); //TODO
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
                if (!ensurePartition(fromPartitionName, toPartitionName)) {
                    continue;
                }
                if (!isElected(stripe)) {
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
                LOG.warn("Failed to get ring partition properties fromPartitionName:{}", fromPartitionName);
                return false;
            }

            if (properties.partitionProperties == null) {
                LOG.warn("Missing partition properties fromPartitionName:{}", fromPartitionName);
                Cursor existing = getPartitionCursor(fromPartitionName, toPartitionName, null);
                Cursor update = (existing != null)
                    ? new Cursor(false, false, existing.maxTimestamp, existing.maxVersion, existing.memberTxIds)
                    : new Cursor(false, false, -1, -1, Maps.newHashMap());
                savePartitionCursor(fromPartitionName, toPartitionName, update);
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
        Cursor existingCursor = getPartitionCursor(partitionTuple.from, toPartitionName, DEFAULT_CURSOR);

        PartitionClient fromClient = partitionClientProvider.getPartition(partitionTuple.from);
        if (!isElected(stripe)) {
            return 0;
        }

        long takeMinTimestamp = toPartitionConfig.startTimestamp <= 0 ? -1 : toPartitionConfig.startTimestamp;
        long takeMaxTimestamp = toPartitionConfig.stopTimestamp <= 0 ? -1 : toPartitionConfig.stopTimestamp;

        long takeMinVersion = toPartitionConfig.startVersion <= 0 ? -1 : toPartitionConfig.startVersion;
        long takeMaxVersion = toPartitionConfig.stopVersion <= 0 ? -1 : toPartitionConfig.stopVersion;

        long timeShiftMillis = toPartitionConfig.timeShiftMillis;

        String readableFromTo = PartitionName.toHumanReadableString(partitionTuple.from) + '/' + PartitionName.toHumanReadableString(partitionTuple.to);
        String statsBytes = "sender/sync/" + readableFromTo + "/bytes";
        String statsCount = "sender/sync/" + readableFromTo + "/count";

        Map<RingMember, Long> cursorMemberTxIds = Maps.newHashMap(existingCursor.memberTxIds);
        AtomicLong cursorMaxTimestamp = new AtomicLong(existingCursor.maxTimestamp);
        AtomicLong cursorMaxVersion = new AtomicLong(existingCursor.maxVersion);

        int synced = 0;
        boolean taking = true;
        while (taking) {
            MutableLong bytesCount = new MutableLong();
            List<Row> rows = Lists.newArrayListWithExpectedSize(config.batchSize);
            long start = System.currentTimeMillis();
            TakeResult takeResult = fromClient.takeFromTransactionId(null,
                cursorMemberTxIds,
                config.batchSize,
                highwater -> {
                    if (highwater != null) {
                        for (WALHighwater.RingMemberHighwater memberHighwater : highwater.ringMemberHighwater) {
                            cursorMemberTxIds.merge(memberHighwater.ringMember, memberHighwater.transactionId, Math::max);
                        }
                    }
                },
                (rowTxId, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    cursorMaxTimestamp.set(Math.max(cursorMaxTimestamp.get(), valueTimestamp));
                    cursorMaxVersion.set(Math.max(cursorMaxVersion.get(), valueVersion));
                    if ((takeMinTimestamp == -1 || valueTimestamp > takeMinTimestamp)
                        && (takeMaxTimestamp == -1 || takeMaxTimestamp > valueTimestamp)
                        && (takeMinVersion == -1 || valueVersion > takeMinVersion)
                        && (takeMaxVersion == -1 || takeMaxVersion > valueVersion)) {
                        rows.add(new Row(prefix, key, value, valueTimestamp + timeShiftMillis, valueTombstoned));
                    }
                    bytesCount.add((key == null ? 0 : key.length) + (value == null ? 0 : value.length));
                    return TxResult.MORE;
                },
                additionalSolverAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.empty());

            if (!isElected(stripe)) {
                return synced;
            }
            if (rows.isEmpty()) {
                //TODO we would prefer to check tookToEnd, but api limitation means an empty partition is indistinguishable from a partial take
                taking = false;
            } else {
                long ingressLatency = System.currentTimeMillis() - start;
                stats.ingressed("sender/sync/bytes", bytesCount.longValue(), 0);
                stats.ingressed(statsBytes, bytesCount.longValue(), ingressLatency);
                stats.ingressed("sender/sync/count", rows.size(), 0);
                stats.ingressed(statsCount, rows.size(), ingressLatency);
                start = System.currentTimeMillis();

                toSyncClient.commitRows(toPartitionName, rows);
                synced += rows.size();

                long egressLatency = System.currentTimeMillis() - start;
                stats.egressed("sender/sync/bytes", bytesCount.longValue(), 0);
                stats.egressed(statsBytes, bytesCount.longValue(), egressLatency);
                stats.egressed("sender/sync/count", rows.size(), 0);
                stats.egressed(statsCount, rows.size(), egressLatency);
            }

            cursorMemberTxIds.merge(takeResult.tookFrom, takeResult.lastTxId, Math::max);
            if (takeResult.tookToEnd != null) {
                for (WALHighwater.RingMemberHighwater ringMemberHighwater : takeResult.tookToEnd.ringMemberHighwater) {
                    cursorMemberTxIds.merge(ringMemberHighwater.ringMember, ringMemberHighwater.transactionId, Math::max);
                }
                taking = false;
            }

            Cursor cursor = new Cursor(true, taking, cursorMaxTimestamp.get(), cursorMaxVersion.get(), cursorMemberTxIds);
            if (!existingCursor.equals(cursor)) {
                savePartitionCursor(partitionTuple.from, toPartitionName, cursor);
                existingCursor = cursor;
            }
        }

        return synced;
    }

    private Cursor getPartitionCursor(PartitionName fromPartitionName, PartitionName toPartitionName, Cursor defaultCursor) throws Exception {
        PartitionClient cursorClient = cursorClient();
        byte[] cursorKey = cursorKey(fromPartitionName, toPartitionName);
        Cursor[] result = new Cursor[1];
        cursorClient.get(Consistency.leader_quorum, null,
            unprefixedWALKeyStream -> unprefixedWALKeyStream.stream(cursorKey),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    result[0] = cursorFromValue(value);
                }
                return true;
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
        return result[0] != null ? result[0] : defaultCursor;
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
        int valueLength = 1 + 1 + 1 + 2;
        for (RingMember ringMember : cursor.memberTxIds.keySet()) {
            valueLength += 2 + ringMember.sizeInBytes() + 8;
        }
        valueLength += 8 + 8;

        byte[] value = new byte[valueLength];
        value[0] = 2; // version
        value[1] = (byte) (cursor.exists ? 1 : 0);
        value[2] = (byte) (cursor.taking ? 1 : 0);
        UIO.unsignedShortBytes(cursor.memberTxIds.size(), value, 3);
        int o = 5;
        for (Entry<RingMember, Long> entry : cursor.memberTxIds.entrySet()) {
            int memberLength = entry.getKey().sizeInBytes();
            UIO.unsignedShortBytes(memberLength, value, o);
            o += 2;
            entry.getKey().toBytes(value, o);
            o += memberLength;
            UIO.longBytes(entry.getValue(), value, o);
            o += 8;
        }
        UIO.longBytes(cursor.maxTimestamp, value, o);
        o += 8;
        UIO.longBytes(cursor.maxVersion, value, o);
        o += 8;
        return value;
    }

    private Cursor cursorFromValue(byte[] value) throws InterruptedException {
        if (value[0] == 2) {
            boolean exists = value[1] == 1;
            boolean taking = value[2] == 1;

            int memberTxIdsLength = UIO.bytesUnsignedShort(value, 3);
            int o = 5;

            Map<RingMember, Long> memberTxIds = Maps.newHashMap();
            for (int i = 0; i < memberTxIdsLength; i++) {
                int memberLength = UIO.bytesUnsignedShort(value, o);
                o += 2;
                RingMember member = amzaInterner.internRingMember(value, o, memberLength);
                o += memberLength;
                long txId = UIO.bytesLong(value, o);
                memberTxIds.put(member, txId);
                o += 8;
            }

            long maxTimestamp = -1;
            long maxVersion = -1;
            if (value.length >= (o + 8 + 8)) {
                maxTimestamp = UIO.bytesLong(value, o);
                o += 8;
                maxVersion = UIO.bytesLong(value, o);
                o += 8;
            }

            return new Cursor(exists, taking, maxTimestamp, maxVersion, memberTxIds);
        } else if (value[0] == 1) {
            boolean taking = value[1] == 1;

            int memberTxIdsLength = UIO.bytesUnsignedShort(value, 2);
            int o = 4;

            Map<RingMember, Long> memberTxIds = Maps.newHashMap();
            for (int i = 0; i < memberTxIdsLength; i++) {
                int memberLength = UIO.bytesUnsignedShort(value, o);
                o += 2;
                RingMember member = amzaInterner.internRingMember(value, o, memberLength);
                o += memberLength;
                long txId = UIO.bytesLong(value, o);
                memberTxIds.put(member, txId);
                o += 8;
            }

            long maxTimestamp = -1;
            long maxVersion = -1;
            if (value.length >= (o + 8 + 8)) {
                maxTimestamp = UIO.bytesLong(value, o);
                o += 8;
                maxVersion = UIO.bytesLong(value, o);
                o += 8;
            }

            return new Cursor(true, taking, maxTimestamp, maxVersion, memberTxIds);
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
        return amzaInterner.internPartitionName(fromPartitionBytes, 0, fromPartitionBytes.length);
    }

    private PartitionName cursorKeyToPartitionName(byte[] key) throws InterruptedException {
        int fromPartitionLength = UIO.bytesUnsignedShort(key, 0);
        int toPartitionLength = UIO.bytesUnsignedShort(key, 2 + fromPartitionLength);
        byte[] toPartitionBytes = new byte[toPartitionLength];
        UIO.readBytes(key, 2 + fromPartitionLength + 2, toPartitionBytes);
        return amzaInterner.internPartitionName(toPartitionBytes, 0, toPartitionBytes.length);
    }
}
