package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.api.stream.KeyContainedStream;
import com.jivesoftware.os.amza.api.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.api.stream.KeyValues;
import com.jivesoftware.os.amza.api.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxFpStream;
import com.jivesoftware.os.amza.api.stream.TxKeyPointers;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.api.stream.WALKeyPointers;
import com.jivesoftware.os.amza.api.stream.WALMergeKeyPointerStream;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexName.Type;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.DatabaseNotFoundException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.jivesoftware.os.amza.api.wal.WALKey.rawKeyKey;
import static com.jivesoftware.os.amza.api.wal.WALKey.rawKeyPrefix;

/**
 * @author jonathan.colt
 */
public class LABPointerIndexWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;

    private final String providerName;
    private final VersionedPartitionName versionedPartitionName;
    private final LABPointerIndexWALIndexName name;
    private final int maxUpdatesBeforeFlush;
    private final LABEnvironment environment;
    private ValueIndex primaryDb;
    private ValueIndex prefixDb;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);
    private final AtomicReference<WALIndex> compactingTo = new AtomicReference<>();

    public LABPointerIndexWALIndex(String providerName,
        VersionedPartitionName versionedPartitionName,
        LABEnvironment environment,
        LABPointerIndexWALIndexName name,
        int maxUpdatesBeforeFlush) throws Exception {
        this.providerName = providerName;
        this.versionedPartitionName = versionedPartitionName;
        this.name = name;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
        this.environment = environment;
        this.primaryDb = environment.open(name.getPrimaryName(), maxUpdatesBeforeFlush, 10 * 1024 * 1024, -1, -1); // TODO config
        this.prefixDb = environment.open(name.getPrefixName(), maxUpdatesBeforeFlush, 10 * 1024 * 1024, -1, -1); // TODO config
    }

    private boolean entryToWALPointer(RowType rowType, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion,
        long timestamp, boolean tombstoned, long version, long pointer,
        KeyValuePointerStream stream) throws Exception {
        return stream.stream(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version, pointer);
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
                    removeDatabase(type);
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
            return pointers.consume((txId, prefix, key, timestamp, tombstoned, version, fp) -> {
                byte[] pk = WALKey.compose(prefix, key);
                return primaryDb.get(key, (key1, timestamp1, tombstoned1, version1, payload) -> {
                    long pointer = (payload == null) ? -1 : UIO.bytesLong(payload);
                    if (pointer != -1) {
                        int c = CompareTimestampVersions.compare(timestamp1, version1, timestamp, version);
                        mode[0] = (c < 0) ? WALMergeKeyPointerStream.clobbered : WALMergeKeyPointerStream.ignored;
                    } else {
                        mode[0] = WALMergeKeyPointerStream.added;
                    }

                    if (mode[0] != WALMergeKeyPointerStream.ignored) {
                        primaryDb.append((pointerStream) -> {
                            return pointerStream.stream(pk, timestamp, tombstoned, version, UIO.longBytes(fp));
                        });

                        if (prefix != null) {
                            UIO.longBytes(txId, txFpBytes, 0);
                            UIO.longBytes(fp, txFpBytes, 8);
                            byte[] prefixTxFp = WALKey.compose(prefix, txFpBytes);
                            prefixDb.append((pointerStream) -> {
                                return pointerStream.stream(prefixTxFp, timestamp, tombstoned, version, UIO.longBytes(fp));
                            });
                        }
                    }
                    if (stream != null) {
                        return stream.stream(mode[0], txId, prefix, key, timestamp, tombstoned, version, fp);
                    } else {
                        return true;
                    }
                });

            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean takePrefixUpdatesSince(byte[] prefix, long sinceTransactionId, TxFpStream txFpStream) throws Exception {
        lock.acquire();
        try {
            byte[] fromFpPk = WALKey.compose(prefix, new byte[0]);
            byte[] toFpPk = WALKey.prefixUpperExclusive(fromFpPk);
            return prefixDb.rangeScan(fromFpPk, toFpPk, (rawKey, timestamp, tombstoned, version, pointer) -> {
                if (KeyUtil.compare(rawKey, toFpPk) >= 0) {
                    return false;
                }
                byte[] key = WALKey.rawKeyKey(rawKey);
                long takeTxId = UIO.bytesLong(key, 0);
                long takeFp = UIO.bytesLong(key, 8);
                return txFpStream.stream(takeTxId, takeFp);
            });
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
            return primaryDb.get(pk, (rawKey, timestamp, tombstoned, version, pointer1) -> {
                return stream.stream(prefix, key, timestamp, tombstoned, version, pointer1 == null ? -1 : UIO.bytesLong(pointer1));
            });
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
                return primaryDb.get(pk, (rawKey, timestamp, tombstoned, version, pointer1) -> {
                    return stream.stream(prefix, key, timestamp, tombstoned, version, pointer1 == null ? -1 : UIO.bytesLong(pointer1));
                });
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        lock.acquire();
        try {
            return keyValues.consume((rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                byte[] pk = WALKey.compose(prefix, key);
                return primaryDb.get(pk, (rawKey, timestamp, tombstoned, version, pointer1) -> {
                    return entryToWALPointer(rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                        timestamp, tombstoned, version, pointer1 == null ? -1 : UIO.bytesLong(pointer1), stream);
                });
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
                (_prefix, _key, timestamp, tombstoned, version, fp) -> {
                    stream.stream(prefix, key, fp != -1 && !tombstoned);
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
            boolean completed = keyPointers.consume((prefix, key, requestTimestamp, requestTombstoned, requestVersion, fp) -> getPointer(prefix, key,
                (_prefix, _key, indexTimestamp, indexTombstoned, indexVersion, indexFp) -> {
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
            primaryDb.commit(fsync);
            prefixDb.commit(fsync);

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
            primaryDb.close();
            primaryDb = null;
            prefixDb.close();
            prefixDb = null;
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public boolean rowScan(final WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        try {
            return primaryDb.rowScan(
                (rawKey, timestamp, tombstoned, version, pointer) -> stream.stream(rawKeyPrefix(rawKey), rawKeyKey(rawKey), timestamp, tombstoned, version,
                    pointer == null ? -1 : UIO.bytesLong(pointer)));
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, WALKeyPointerStream stream) throws Exception {
        lock.acquire();

        try {
            byte[] fromPk = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
            byte[] toPk = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
            return primaryDb.rangeScan(fromPk, toPk,
                (rawKey, timestamp, tombstoned, version, pointer) -> stream.stream(rawKeyPrefix(rawKey), rawKeyKey(rawKey), timestamp, tombstoned, version,
                    pointer == null ? -1 : UIO.bytesLong(pointer)));

        } finally {
            lock.release();
        }
    }

    @Override
    public CompactionWALIndex startCompaction(boolean hasActive) throws Exception {

        synchronized (compactingTo) {
            WALIndex got = compactingTo.get();
            if (got != null) {
                throw new IllegalStateException("Tried to compact while another compaction is already underway: " + name);
            }

            if (primaryDb == null || prefixDb == null) {
                throw new IllegalStateException("Tried to compact a index that has been expunged: " + name);
            }

            if (!hasActive) {
                removeDatabase(Type.active);
            }
            removeDatabase(Type.compacting);
            removeDatabase(Type.compacted);
            removeDatabase(Type.backup);

            final LABPointerIndexWALIndex compactingWALIndex = new LABPointerIndexWALIndex(providerName, versionedPartitionName, environment,
                name.typeName(Type.compacting), maxUpdatesBeforeFlush);
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
                        compactingTo.set(null);
                        if (primaryDb == null || prefixDb == null) {
                            LOG.warn("Was not commited because index has been closed.");
                        } else {
                            LOG.info("Committing before swap: {}", name.getPrimaryName());

                            compactingWALIndex.commit(fsync);
                            compactingWALIndex.close();
                            rename(Type.compacting, Type.compacted);

                            primaryDb.close();
                            primaryDb = null;
                            prefixDb.close();
                            prefixDb = null;
                            if (hasActive) {
                                rename(Type.active, Type.backup);
                            }

                            if (commit != null) {
                                commit.call();
                            }

                            rename(Type.compacted, Type.active);
                            removeDatabase(Type.backup);

                            primaryDb = environment.open(name.getPrimaryName(), maxUpdatesBeforeFlush, 10 * 1024 * 1024, -1, -1);
                            prefixDb = environment.open(name.getPrefixName(), maxUpdatesBeforeFlush, 10 * 1024 * 1024, -1, -1);

                            LOG.info("Committing after swap: {}", name.getPrimaryName());
                        }
                    } finally {
                        lock.release(numPermits);
                    }
                }
            };
        }
    }

    private void rename(Type fromType, Type toType) throws Exception {
        environment.rename(name.typeName(fromType).getPrimaryName(), name.typeName(toType).getPrimaryName());
        environment.rename(name.typeName(fromType).getPrefixName(), name.typeName(toType).getPrefixName());
    }

    private void removeDatabase(Type type) throws Exception {
        try {
            environment.remove(name.typeName(type).getPrimaryName());
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
        try {
            environment.remove(name.typeName(type).getPrefixName());
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
    }

    @Override
    public void updatedProperties(Map<String, String> properties) {
    }

    @Override
    public String toString() {
        return "LABPointerIndexWALIndex{" + "name=" + name
            + ", environment=" + environment
            + ", primaryDb=" + primaryDb
            + ", prefixDb=" + prefixDb
            + ", lock=" + lock
            + ", count=" + count
            + ", commits=" + commits
            + ", compactingTo=" + compactingTo
            + '}';
    }

    public void flush(boolean fsync) throws Exception {
        primaryDb.commit(fsync);
        prefixDb.commit(fsync);
    }

}
