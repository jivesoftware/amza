package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.TxFpStream;
import com.jivesoftware.os.amza.api.stream.TxKeyPointers;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.WALKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALMergeKeyPointerStream;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexName.Type;
import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABRawhide;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class LABPointerIndexWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;

    private final String providerName;
    private final int maxValueSizeInIndex;
    private final VersionedPartitionName versionedPartitionName;
    private final LABPointerIndexWALIndexName name;
    private final LABPointerIndexConfig config;
    private final LABEnvironment[] environments;
    private volatile int currentStripe;
    private ValueIndex primaryDb;
    private ValueIndex prefixDb;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);
    private final AtomicReference<WALIndex> compactingTo = new AtomicReference<>();

    public LABPointerIndexWALIndex(String providerName,
        int maxValueSizeInIndex,
        VersionedPartitionName versionedPartitionName,
        LABEnvironment[] environments,
        int currentStripe,
        LABPointerIndexWALIndexName name,
        LABPointerIndexConfig config) throws Exception {
        this.providerName = providerName;
        this.maxValueSizeInIndex = maxValueSizeInIndex;
        this.versionedPartitionName = versionedPartitionName;
        this.name = name;
        this.config = config;
        this.environments = environments;
        this.currentStripe = currentStripe;
        this.primaryDb = environments[currentStripe].open(new ValueIndexConfig(name.getPrimaryName(),
            config.getEntriesBetweenLeaps(),
            config.getMaxHeapPressureInBytes(),
            config.getSplitWhenKeysTotalExceedsNBytes(),
            config.getSplitWhenValuesTotalExceedsNBytes(),
            config.getSplitWhenValuesAndKeysTotalExceedsNBytes(),
            NoOpFormatTransformerProvider.NAME,
            LABRawhide.NAME,
            MemoryRawEntryFormat.NAME));
        this.prefixDb = environments[currentStripe].open(new ValueIndexConfig(name.getPrefixName(),
            config.getEntriesBetweenLeaps(),
            config.getMaxHeapPressureInBytes(),
            config.getSplitWhenKeysTotalExceedsNBytes(),
            config.getSplitWhenValuesTotalExceedsNBytes(),
            config.getSplitWhenValuesAndKeysTotalExceedsNBytes(),
            NoOpFormatTransformerProvider.NAME,
            LABRawhide.NAME,
            MemoryRawEntryFormat.NAME));
    }

    @Override
    public int getStripe() {
        return currentStripe;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    public VersionedPartitionName getVersionedPartitionName() {
        return versionedPartitionName;
    }

    @Override
    public void delete() throws Exception {
        close();
        lock.acquire(numPermits);
        try {
            synchronized (compactingTo) {
                WALIndex wali = compactingTo.get();
                if (wali != null) {
                    wali.close();
                }
                for (Type type : Type.values()) {
                    removeDatabase(currentStripe, type);
                }
            }
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        try {
            lock.acquire();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        try {
            byte[] mode = new byte[1];
            byte[] txFpBytes = new byte[16];
            BolBuffer entryBuffer = new BolBuffer();
            BolBuffer keyBuffer = new BolBuffer();
            return pointers.consume((txId, prefix, key, value, timestamp, tombstoned, version, fp) -> {
                byte[] pk = WALKey.compose(prefix, key);
                return primaryDb.get(
                    (stream1) -> stream1.key(0, key, 0, key.length),
                    (index1, key1, timestamp1, tombstoned1, version1, payload) -> {
                        long pointer = (payload == null) ? -1 : payload.getLong(0);
                        if (pointer != -1) {
                            int c = CompareTimestampVersions.compare(timestamp1, version1, timestamp, version);
                            mode[0] = (c < 0) ? WALMergeKeyPointerStream.clobbered : WALMergeKeyPointerStream.ignored;
                        } else {
                            mode[0] = WALMergeKeyPointerStream.added;
                        }

                        if (mode[0] != WALMergeKeyPointerStream.ignored) {
                            byte[] mergePayload = toPayload(fp, value);
                            primaryDb.append((pointerStream) -> {
                                return pointerStream.stream(-1, pk, timestamp, tombstoned, version, mergePayload);
                            }, true, entryBuffer, keyBuffer);

                            if (prefix != null) {
                                UIO.longBytes(txId, txFpBytes, 0);
                                UIO.longBytes(fp, txFpBytes, 8);
                                byte[] prefixTxFp = WALKey.compose(prefix, txFpBytes);
                                prefixDb.append((pointerStream) -> {
                                    return pointerStream.stream(-1, prefixTxFp, timestamp, tombstoned, version, mergePayload);
                                }, true, entryBuffer, keyBuffer);
                            }
                        }
                        if (stream != null) {
                            return stream.stream(mode[0], txId, prefix, key, timestamp, tombstoned, version, fp);
                        } else {
                            return true;
                        }
                    },
                    true);

            });
        } finally {
            lock.release();
        }
    }

    private static byte PAYLOAD_NULL = -1;
    private static byte PAYLOAD_NONNULL = -2;

    private byte[] toPayload(long fp, byte[] value) {
        if (fp < 0) {
            throw new IllegalArgumentException("Negative fp " + fp);
        }
        int valueLength = (value == null) ? 0 : value.length;
        if (maxValueSizeInIndex >= 0 && maxValueSizeInIndex >= valueLength) {
            // leverage the fact that fp cannot be negative by using a negative leading byte
            byte[] payload = new byte[1 + (value == null ? 0 : value.length)];
            payload[0] = (value == null) ? PAYLOAD_NULL : PAYLOAD_NONNULL;
            if (value != null && value.length > 0) {
                System.arraycopy(value, 0, payload, 1, value.length);
            }
            return payload;
        } else {
            return UIO.longBytes(fp);
        }
    }

    private boolean fromPayload(long txId,
        long fp,
        byte[] payload,
        TxFpStream txFpStream,
        boolean hydrateValues) throws Exception {
        if (payload != null && payload[0] < 0) {
            if (payload[0] == PAYLOAD_NULL) {
                return txFpStream.stream(txId, fp, true, null);
            } else if (payload[0] == PAYLOAD_NONNULL) {
                byte[] value = new byte[payload.length - 1];
                System.arraycopy(payload, 1, value, 0, value.length);
                return txFpStream.stream(txId, fp, true, value);
            }
        }
        return txFpStream.stream(txId, fp, !hydrateValues, null);
    }

    private boolean fromPayload(byte[] prefix,
        byte[] key,
        long timestamp,
        boolean tombstoned,
        long version,
        byte[] payload,
        WALKeyPointerStream stream,
        boolean hydrateValues) throws Exception {
        if (payload != null) {
            if (payload[0] == PAYLOAD_NULL) {
                return stream.stream(prefix, key, timestamp, tombstoned, version, -1, true, null);
            } else if (payload[0] == PAYLOAD_NONNULL) {
                byte[] value = new byte[payload.length - 1];
                System.arraycopy(payload, 1, value, 0, value.length);
                return stream.stream(prefix, key, timestamp, tombstoned, version, -1, true, value);
            } else {
                long fp = UIO.bytesLong(payload);
                return stream.stream(prefix, key, timestamp, tombstoned, version, fp, false, null);
            }
        }
        return stream.stream(prefix, key, timestamp, tombstoned, version, -1, !hydrateValues, null);
    }

    private boolean fromPayload(byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        long valueVersion,
        long timestamp,
        boolean tombstoned,
        long version,
        byte[] payload,
        KeyValuePointerStream stream,
        boolean hydrateValues) throws Exception {
        if (payload != null) {
            if (payload[0] == PAYLOAD_NULL) {
                return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version, -1, true, null);
            } else if (payload[0] == PAYLOAD_NONNULL) {
                byte[] pointerValue = new byte[payload.length - 1];
                System.arraycopy(payload, 1, pointerValue, 0, pointerValue.length);
                return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version, -1, true, pointerValue);
            } else {
                long fp = UIO.bytesLong(payload);
                return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version, fp, false, null);
            }
        }
        return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version, -1, !hydrateValues, null);
    }

    @Override
    public boolean takePrefixUpdatesSince(byte[] prefix, long sinceTransactionId, TxFpStream txFpStream) throws Exception {
        lock.acquire();
        try {
            byte[] fromFpPk = WALKey.compose(prefix, new byte[0]);
            byte[] toFpPk = WALKey.prefixUpperExclusive(fromFpPk);
            ByteBuffer bbToFpPk = ByteBuffer.wrap(toFpPk);
            return prefixDb.rangeScan(fromFpPk, toFpPk, (index, rawKey, timestamp, tombstoned, version, payload) -> {
                if (IndexUtil.compare(rawKey, bbToFpPk) >= 0) {
                    return false;
                }
                ByteBuffer key = WALKey.rawKeyKey(rawKey);
                long takeTxId = key.getLong(0);
                long takeFp = key.getLong(8);
                return fromPayload(takeTxId, takeFp, IndexUtil.toByteArray(payload), txFpStream, true);
            }, true);
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointer(byte[] prefix, byte[] key, WALKeyPointerStream stream) throws Exception {
        try {
            lock.acquire();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        try {
            byte[] pk = WALKey.compose(prefix, key);
            return primaryDb.get((keyStream) -> keyStream.key(0, pk, 0, pk.length),
                (index, rawKey, timestamp, tombstoned, version, payload) -> {
                    return fromPayload(prefix, key, timestamp, tombstoned, version, IndexUtil.toByteArray(payload), stream, true);
                },
                true);
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(byte[] prefix, UnprefixedWALKeys keys, WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        try {
            return keys.consume((key) -> {
                byte[] pk = WALKey.compose(prefix, key);
                return primaryDb.get((keyStream) -> keyStream.key(0, pk, 0, pk.length),
                    (index, rawKey, timestamp, tombstoned, version, payload) -> {
                        return fromPayload(prefix, key, timestamp, tombstoned, version, IndexUtil.toByteArray(payload), stream, true);
                    },
                    true);
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        lock.acquire();
        try {
            return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                byte[] pk = WALKey.compose(prefix, key);
                return primaryDb.get((keyStream) -> keyStream.key(0, pk, 0, pk.length),
                    (index, rawKey, timestamp, tombstoned, version, payload) -> {
                        return fromPayload(prefix,
                            key,
                            value,
                            valueTimestamp,
                            valueTombstoned,
                            valueVersion,
                            timestamp,
                            tombstoned,
                            version,
                            IndexUtil.toByteArray(payload),
                            stream,
                            true);
                    },
                    true);
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        lock.acquire();
        try {
            return keys.consume((key) -> getPointer(prefix, key,
                (_prefix, _key, timestamp, tombstoned, version, fp, indexValue, value) -> {
                    boolean contained = fp != -1 && !tombstoned;
                    stream.stream(prefix, key, contained, timestamp, version);
                    return true;
                }));
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        lock.acquire();
        try {
            return primaryDb.isEmpty();
        } finally {
            lock.release();
        }
    }

    @Override
    public long deltaCount(WALKeyPointers keyPointers) throws Exception {
        lock.acquire();
        try {
            long[] delta = new long[1];
            boolean completed = keyPointers.consume(
                (prefix, key, requestTimestamp, requestTombstoned, requestVersion, requestFp, requestIndexValue, requestValue)
                -> getPointer(prefix, key, (_prefix, _key, indexTimestamp, indexTombstoned, indexVersion, indexFp, _indexValue, _value) -> {
                    // indexFp, indexTombstoned, requestTombstoned, delta
                    // -1       false            false              1
                    // -1       false            true               0
                    //  1       false            false              0
                    //  1       false            true               -1
                    //  1       true             false              1
                    //  1       true             true               0
                    if (!requestTombstoned && (indexFp == -1 && !indexTombstoned || indexFp != -1 && indexTombstoned)) {
                        delta[0]++;
                    } else if (indexFp != -1 && !indexTombstoned && requestTombstoned) {
                        delta[0]--;
                    }
                    return true;
                }));
            if (!completed) {
                return -1;
            }
            return delta[0];
        } finally {
            lock.release();
        }
    }

    @Override
    public void commit(boolean fsync) throws Exception {
        lock.acquire();
        try {
            // TODO is this the right thing to do?
            primaryDb.commit(fsync, true);
            prefixDb.commit(fsync, true);

            synchronized (commits) {
                count.set(-1);
                commits.incrementAndGet();
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public void close() throws Exception {
        lock.acquire(numPermits);
        try {
            primaryDb.close(true, true);
            primaryDb = null;
            prefixDb.close(true, true);
            prefixDb = null;
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public boolean rowScan(final WALKeyPointerStream stream, boolean hydrateValues) throws Exception {
        lock.acquire();
        try {
            return primaryDb.rowScan(
                (index, rawKey, timestamp, tombstoned, version, payload) -> {
                    byte[] rawKeyBytes = IndexUtil.toByteArray(rawKey);
                    return fromPayload(WALKey.rawKeyPrefix(rawKeyBytes),
                        WALKey.rawKeyKey(rawKeyBytes),
                        timestamp,
                        tombstoned,
                        version,
                        IndexUtil.toByteArray(payload),
                        stream,
                        hydrateValues);
                },
                hydrateValues);
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        WALKeyPointerStream stream,
        boolean hydrateValues) throws Exception {

        lock.acquire();
        try {
            byte[] fromPk = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
            byte[] toPk = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
            return primaryDb.rangeScan(fromPk,
                toPk,
                (index, rawKey, timestamp, tombstoned, version, payload) -> {
                    byte[] rawKeyBytes = IndexUtil.toByteArray(rawKey);
                    return fromPayload(WALKey.rawKeyPrefix(rawKeyBytes),
                        WALKey.rawKeyKey(rawKeyBytes),
                        timestamp,
                        tombstoned,
                        version,
                         IndexUtil.toByteArray(payload),
                        stream,
                        hydrateValues);
                },
                hydrateValues);
        } finally {
            lock.release();
        }
    }

    @Override
    public CompactionWALIndex startCompaction(boolean hasActive, int compactionStripe) throws Exception {

        synchronized (compactingTo) {
            WALIndex got = compactingTo.get();
            if (got != null) {
                throw new IllegalStateException("Tried to compact while another compaction is already underway: " + name);
            }

            if (primaryDb == null || prefixDb == null) {
                throw new IllegalStateException("Tried to compact a index that has been expunged: " + name);
            }

            removeDatabase(compactionStripe, Type.compacting);
            removeDatabase(compactionStripe, Type.compacted);
            removeDatabase(currentStripe, Type.backup);

            final LABPointerIndexWALIndex compactingWALIndex = new LABPointerIndexWALIndex(providerName,
                maxValueSizeInIndex,
                versionedPartitionName,
                environments,
                compactionStripe,
                name.typeName(Type.compacting),
                config);
            compactingTo.set(compactingWALIndex);

            return new CompactionWALIndex() {

                @Override
                public boolean merge(TxKeyPointers pointers) throws Exception {
                    return compactingWALIndex.merge(pointers, null);
                }

                @Override
                public void commit(boolean fsync, Callable<Void> commit) throws Exception {
                    lock.acquire(numPermits);
                    try {
                        compactingWALIndex.commit(fsync);
                        compactingWALIndex.close();
                        if (!compactingTo.compareAndSet(compactingWALIndex, null)) {
                            throw new IllegalStateException("Tried to commit a stale compaction index");
                        }
                        if (primaryDb == null || prefixDb == null) {
                            LOG.warn("Was not commited because index has been closed.");
                        } else {
                            LOG.debug("Committing before swap: {}", name.getPrimaryName());

                            boolean compactedNonEmpty = rename(compactionStripe, Type.compacting, Type.compacted, false);

                            primaryDb.close(true, true);
                            primaryDb = null;
                            prefixDb.close(true, true);
                            prefixDb = null;
                            if (hasActive) {
                                rename(currentStripe, Type.active, Type.backup, compactedNonEmpty);
                            } else {
                                removeDatabase(currentStripe, Type.active);
                            }

                            if (commit != null) {
                                commit.call();
                            }

                            if (compactedNonEmpty) {
                                rename(compactionStripe, Type.compacted, Type.active, true);
                            }
                            removeDatabase(currentStripe, Type.backup);

                            primaryDb = environments[compactionStripe].open(new ValueIndexConfig(name.getPrimaryName(),
                                config.getEntriesBetweenLeaps(),
                                config.getMaxHeapPressureInBytes(),
                                config.getSplitWhenKeysTotalExceedsNBytes(),
                                config.getSplitWhenValuesTotalExceedsNBytes(),
                                config.getSplitWhenValuesAndKeysTotalExceedsNBytes(),
                                NoOpFormatTransformerProvider.NAME,
                                LABRawhide.NAME,
                                MemoryRawEntryFormat.NAME));

                            prefixDb = environments[compactionStripe].open(new ValueIndexConfig(name.getPrefixName(),
                                config.getEntriesBetweenLeaps(),
                                config.getMaxHeapPressureInBytes(),
                                config.getSplitWhenKeysTotalExceedsNBytes(),
                                config.getSplitWhenValuesTotalExceedsNBytes(),
                                config.getSplitWhenValuesAndKeysTotalExceedsNBytes(),
                                NoOpFormatTransformerProvider.NAME,
                                LABRawhide.NAME,
                                MemoryRawEntryFormat.NAME));

                            currentStripe = compactionStripe;
                            LOG.debug("Committing after swap: {}", name.getPrimaryName());
                        }
                    } finally {
                        lock.release(numPermits);
                    }
                }

                @Override
                public void abort() throws Exception {
                    compactingWALIndex.close();
                    if (compactingTo.compareAndSet(compactingWALIndex, null)) {
                        removeDatabase(compactionStripe, Type.compacting);
                    }
                }
            };

        }
    }

    private boolean rename(int stripe, Type fromType, Type toType, boolean required) throws Exception {
        boolean primaryRenamed = environments[stripe].rename(name.typeName(fromType).getPrimaryName(), name.typeName(toType).getPrimaryName());
        boolean prefixRenamed = environments[stripe].rename(name.typeName(fromType).getPrefixName(), name.typeName(toType).getPrefixName());
        if (!primaryRenamed && (required || prefixRenamed)) {
            throw new IOException("Failed to rename"
                + " from:" + name.typeName(fromType).getPrimaryName()
                + " to:" + name.typeName(toType).getPrimaryName()
                + " required:" + required
                + " prefix:" + prefixRenamed);
        }
        return primaryRenamed;
    }

    private void removeDatabase(int stripe, Type type) throws Exception {
        environments[stripe].remove(name.typeName(type).getPrimaryName());
        environments[stripe].remove(name.typeName(type).getPrefixName());
    }

    public void flush(boolean fsync) throws Exception {
        lock.acquire();
        try {
            primaryDb.commit(fsync, true);
            prefixDb.commit(fsync, true);
        } finally {
            lock.release();
        }
    }

    @Override
    public void updatedProperties(Map<String, String> properties) {
    }

    @Override
    public String toString() {
        return "LABPointerIndexWALIndex{" + "name=" + name
            + ", environments=" + Arrays.toString(environments)
            + ", primaryDb=" + primaryDb
            + ", prefixDb=" + prefixDb
            + ", lock=" + lock
            + ", count=" + count
            + ", commits=" + commits
            + ", compactingTo=" + compactingTo
            + '}';
    }

}
