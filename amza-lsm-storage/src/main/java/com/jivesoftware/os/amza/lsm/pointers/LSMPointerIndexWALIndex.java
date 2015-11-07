package com.jivesoftware.os.amza.lsm.pointers;

import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.lsm.pointers.LSMPointerIndexWALIndexName.Type;
import com.jivesoftware.os.amza.lsm.pointers.api.NextPointer;
import com.jivesoftware.os.amza.lsm.pointers.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.pointers.api.PointerStream;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.scan.CompactionWALIndex;
import com.jivesoftware.os.amza.shared.stream.KeyContainedStream;
import com.jivesoftware.os.amza.shared.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.shared.stream.KeyValues;
import com.jivesoftware.os.amza.shared.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.stream.TxFpStream;
import com.jivesoftware.os.amza.shared.stream.TxKeyPointers;
import com.jivesoftware.os.amza.shared.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.stream.WALKeyPointers;
import com.jivesoftware.os.amza.shared.stream.WALMergeKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.KeyUtil;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.DatabaseNotFoundException;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.jivesoftware.os.amza.shared.wal.WALKey.rawKeyKey;
import static com.jivesoftware.os.amza.shared.wal.WALKey.rawKeyPrefix;

/**
 * @author jonathan.colt
 */
public class LSMPointerIndexWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;

    private final LSMPointerIndexWALIndexName name;
    private final int maxUpdatesBetweenCompactionHintMarker;
    private final LSMPointerIndexEnvironment environment;
    private PointerIndex primaryDb;
    private PointerIndex prefixDb;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);
    private final AtomicReference<WALIndex> compactingTo = new AtomicReference<>();

    public LSMPointerIndexWALIndex(LSMPointerIndexEnvironment environment,
        LSMPointerIndexWALIndexName name,
        int maxUpdatesBetweenCompactionHintMarker) throws Exception {
        this.name = name;
        this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
        this.environment = environment;
        this.primaryDb = environment.open(name.getPrimaryName(), maxUpdatesBetweenCompactionHintMarker);
        this.prefixDb = environment.open(name.getPrefixName(), maxUpdatesBetweenCompactionHintMarker);
    }

    private boolean entryToWALPointer(byte[] prefix, byte[] key, long valueTimestamp, boolean valueTombstoned, long valueVersion, long pointer,
        WALKeyPointerStream stream) throws Exception {
        return stream.stream(prefix, key, valueTimestamp, valueTombstoned, valueVersion, pointer);
    }

    private boolean entryToWALPointer(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion,
        long timestamp, boolean tombstoned, long version, long pointer,
        KeyValuePointerStream stream) throws Exception {
        return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version, pointer);
    }

    @Override
    public boolean delete() throws Exception {
        close();
        lock.acquire(numPermits);
        try {
            synchronized (compactingTo) {
                WALIndex wali = compactingTo.get();
                if (wali != null) {
                    wali.close();
                }
                removeDatabase(Type.active);
                removeDatabase(Type.backup);
                removeDatabase(Type.compacted);
                removeDatabase(Type.compacting);
                return true;
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
                NextPointer got = primaryDb.getPointer(key);
                got.next((key1, timestamp1, tombstoned1, version1, pointer) -> {
                    if (pointer != -1) {
                        int c = CompareTimestampVersions.compare(timestamp1, version1, timestamp, version);
                        mode[0] = (c < 0) ? WALMergeKeyPointerStream.clobbered : WALMergeKeyPointerStream.ignored;
                    } else {
                        mode[0] = WALMergeKeyPointerStream.added;
                    }

                    if (mode[0] != WALMergeKeyPointerStream.ignored) {
                        primaryDb.append((pointerStream) -> {
                            return pointerStream.stream(pk, timestamp, tombstoned, version, fp);
                        });

                        if (prefix != null) {
                            UIO.longBytes(txId, txFpBytes, 0);
                            UIO.longBytes(fp, txFpBytes, 8);
                            byte[] prefixTxFp = WALKey.compose(prefix, txFpBytes);
                            prefixDb.append((pointerStream) -> {
                                return pointerStream.stream(prefixTxFp, timestamp, tombstoned, version, fp);
                            });
                        }
                    }
                    if (stream != null) {
                        return stream.stream(mode[0], txId, prefix, key, timestamp, tombstoned, version, fp);
                    } else {
                        return true;
                    }
                });
                return true;
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
            NextPointer rangeScan = prefixDb.rangeScan(fromFpPk, toFpPk);
            PointerStream stream = (rawKey, timestamp, tombstoned, version, pointer) -> {
                if (KeyUtil.compare(rawKey, toFpPk) >= 0) {
                    return false;
                }
                byte[] key = WALKey.rawKeyKey(rawKey);
                long takeTxId = UIO.bytesLong(key, 0);
                long takeFp = UIO.bytesLong(key, 8);
                return txFpStream.stream(takeTxId, takeFp);
            };
            while (rangeScan.next(stream));
            return false;

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
            NextPointer pointer = primaryDb.getPointer(pk);
            return pointer.next((rawKey, timestamp, tombstoned, version, pointer1) -> {
                return stream.stream(prefix, key, timestamp, tombstoned, version, pointer1);
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
                NextPointer pointer = primaryDb.getPointer(pk);
                return pointer.next((rawKey, timestamp, tombstoned, version, pointer1) -> {
                    return stream.stream(prefix, key, timestamp, tombstoned, version, pointer1);
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
            return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                byte[] pk = WALKey.compose(prefix, key);
                NextPointer pointer = primaryDb.getPointer(pk);
                return pointer.next((rawKey, timestamp, tombstoned, version, pointer1) -> {
                    return entryToWALPointer(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion,
                        timestamp, tombstoned, version, pointer1, stream);
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

//    @Override
//    public long size() throws Exception {
//        lock.acquire();
//        try {
//            long size = count.get();
//            if (size >= 0) {
//                return size;
//            }
//            int numCommits = commits.get();
//            size = primaryDb.count();
//            synchronized (commits) {
//                if (numCommits == commits.get()) {
//                    count.set(size);
//                }
//            }
//            return size;
//        } finally {
//            lock.release();
//        }
//    }
    @Override
    public void commit() throws Exception {
        lock.acquire();
        try {
            // TODO is this the right thing to do?
            primaryDb.commit();
            prefixDb.commit();

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
            return stream(stream, primaryDb.rowScan());
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
            return stream(stream, primaryDb.rangeScan(fromPk, toPk));

        } finally {
            lock.release();
        }
    }

    private boolean stream(final WALKeyPointerStream stream, NextPointer nextPointer) throws Exception {
        PointerStream pointerStream = (rawKey, timestamp, tombstoned, version, pointer)
            -> stream.stream(rawKeyPrefix(rawKey), rawKeyKey(rawKey), timestamp, tombstoned, version, pointer);
        while (nextPointer.next(pointerStream));
        return false;
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {

        synchronized (compactingTo) {
            WALIndex got = compactingTo.get();
            if (got != null) {
                throw new IllegalStateException("Tried to compact while another compaction is already underway: " + name);
            }

            if (primaryDb == null || prefixDb == null) {
                throw new IllegalStateException("Tried to compact a index that has been expunged: " + name);
            }

            removeDatabase(Type.compacting);
            removeDatabase(Type.compacted);
            removeDatabase(Type.backup);

            final LSMPointerIndexWALIndex compactingWALIndex = new LSMPointerIndexWALIndex(environment,
                name.typeName(Type.compacting),
                maxUpdatesBetweenCompactionHintMarker);
            compactingTo.set(compactingWALIndex);

            return new CompactionWALIndex() {

                @Override
                public boolean merge(TxKeyPointers pointers) throws Exception {
                    return compactingWALIndex.merge(pointers, null);
                }

                @Override
                public void abort() throws Exception {
                    try {
                        compactingTo.set(null);
                        compactingWALIndex.close();
                    } catch (IOException ex) {
                        throw new RuntimeException();
                    }
                }

                @Override
                public void commit() throws Exception {
                    lock.acquire(numPermits);
                    try {
                        compactingTo.set(null);
                        if (primaryDb == null || prefixDb == null) {
                            LOG.warn("Was not commited because index has been closed.");
                        } else {
                            LOG.info("Committing before swap: {}", name.getPrimaryName());

                            compactingWALIndex.commit();
                            compactingWALIndex.close();
                            rename(Type.compacting, Type.compacted);

                            primaryDb.close();
                            primaryDb = null;
                            prefixDb.close();
                            prefixDb = null;
                            rename(Type.active, Type.backup);

                            rename(Type.compacted, Type.active);
                            removeDatabase(Type.backup);

                            primaryDb = environment.open(name.getPrimaryName(), maxUpdatesBetweenCompactionHintMarker);
                            prefixDb = environment.open(name.getPrefixName(), maxUpdatesBetweenCompactionHintMarker);

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
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

    @Override
    public String toString() {
        return "LSMPointerIndexWALIndex{" + "name=" + name
            + ", environment=" + environment
            + ", primaryDb=" + primaryDb
            + ", prefixDb=" + prefixDb
            + ", lock=" + lock
            + ", count=" + count
            + ", commits=" + commits
            + ", compactingTo=" + compactingTo
            + '}';
    }

}
