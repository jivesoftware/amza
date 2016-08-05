package com.jivesoftware.os.amza.berkeleydb;

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
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexName.Type;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class BerkeleyDBWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;

    private final String providerName;
    private final VersionedPartitionName versionedPartitionName;
    private final Environment[] environments;
    private final BerkeleyDBWALIndexName name;
    private volatile int currentStripe;
    private final DatabaseConfig primaryDbConfig;
    private final DatabaseConfig prefixDbConfig;
    private Database primaryDb;
    private Database prefixDb;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);
    private final AtomicReference<WALIndex> compactingTo = new AtomicReference<>();

    public BerkeleyDBWALIndex(String providerName,
        VersionedPartitionName versionedPartitionName,
        Environment[] environments,
        BerkeleyDBWALIndexName name,
        int currentStripe) throws Exception {

        this.providerName = providerName;
        this.versionedPartitionName = versionedPartitionName;
        this.environments = environments;
        this.name = name;
        this.currentStripe = currentStripe;

        // Open the database, creating one if it does not exist
        this.primaryDbConfig = new DatabaseConfig()
            .setAllowCreate(true)
            .setBtreeComparator(KeyUtil.lexicographicalComparator())
            .setOverrideBtreeComparator(true);
        this.primaryDb = environments[currentStripe].openDatabase(null, name.getPrimaryName(), primaryDbConfig);

        // Open the database, creating one if it does not exist
        this.prefixDbConfig = new DatabaseConfig()
            .setAllowCreate(true)
            .setBtreeComparator(KeyUtil.lexicographicalComparator())
            .setOverrideBtreeComparator(true);
        this.prefixDb = environments[currentStripe].openDatabase(null, name.getPrefixName(), prefixDbConfig);
    }

    private void walPointerToEntry(long fp, long timestamp, boolean tombstoned, long version, DatabaseEntry dbValue) {
        byte[] valueBytes = UIO.longBytes(fp);
        byte[] entryBytes = new byte[valueBytes.length + 8 + 1 + 8];
        System.arraycopy(valueBytes, 0, entryBytes, 0, valueBytes.length);
        UIO.longBytes(timestamp, entryBytes, valueBytes.length);
        entryBytes[valueBytes.length + 8] = tombstoned ? (byte) 1 : (byte) 0;
        UIO.longBytes(version, entryBytes, valueBytes.length + 8 + 1);
        dbValue.setData(entryBytes);
    }

    private boolean entryToWALPointer(byte[] prefix, byte[] key, byte[] entryBytes, WALKeyPointerStream stream, boolean hydrateValues) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1 - 8];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        long version = UIO.bytesLong(entryBytes, valueBytes.length + 8 + 1);
        return stream.stream(prefix, key, timestamp, tombstoned, version, UIO.bytesLong(valueBytes), !hydrateValues, null);
    }

    private boolean entryToWALPointer(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion,
        byte[] entryBytes, KeyValuePointerStream stream) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1 - 8];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        long version = UIO.bytesLong(entryBytes, valueBytes.length + 8 + 1);
        return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, timestamp, tombstoned, version,
            UIO.bytesLong(valueBytes), false, null);
    }

    private long entryToTimestamp(byte[] entryBytes) throws Exception {
        int valueLength = entryBytes.length - 8 - 1 - 8;
        return UIO.bytesLong(entryBytes, valueLength);
    }

    private long entryToVersion(byte[] entryBytes) throws Exception {
        int valueLength = entryBytes.length - 8 - 1 - 8;
        return UIO.bytesLong(entryBytes, valueLength + 1 + 8);
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
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dbValue = new DatabaseEntry();
            byte[] txFpBytes = new byte[16];
            byte[] emptyValue = new byte[0];
            return pointers.consume((txId, prefix, key, value, timestamp, tombstoned, version, fp) -> {
                byte[] pk = WALKey.compose(prefix, key);
                dbKey.setData(pk);
                OperationStatus status = primaryDb.get(null, dbKey, dbValue, LockMode.READ_UNCOMMITTED);
                byte mode;
                if (status == OperationStatus.SUCCESS) {
                    int c = CompareTimestampVersions.compare(entryToTimestamp(dbValue.getData()), entryToVersion(dbValue.getData()), timestamp, version);
                    mode = (c < 0) ? WALMergeKeyPointerStream.clobbered : WALMergeKeyPointerStream.ignored;
                } else {
                    mode = WALMergeKeyPointerStream.added;
                }
                if (mode != WALMergeKeyPointerStream.ignored) {
                    walPointerToEntry(fp, timestamp, tombstoned, version, dbValue);
                    primaryDb.put(null, dbKey, dbValue);

                    if (prefix != null) {
                        UIO.longBytes(txId, txFpBytes, 0);
                        UIO.longBytes(fp, txFpBytes, 8);
                        byte[] prefixTxFp = WALKey.compose(prefix, txFpBytes);
                        dbKey.setData(prefixTxFp);
                        dbValue.setData(emptyValue);
                        prefixDb.put(null, dbKey, dbValue);
                    }
                }
                if (stream != null) {
                    return stream.stream(mode, txId, prefix, key, timestamp, tombstoned, version, fp);
                } else {
                    return true;
                }
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean takePrefixUpdatesSince(byte[] prefix, long sinceTransactionId, TxFpStream txFpStream) throws Exception {
        lock.acquire();
        try (Cursor cursor = prefixDb.openCursor(null, null)) {
            byte[] fromFpPk = WALKey.compose(prefix, new byte[0]);
            byte[] toFpPk = WALKey.prefixUpperExclusive(fromFpPk);
            DatabaseEntry keyEntry = new DatabaseEntry(fromFpPk);
            DatabaseEntry valueEntry = new DatabaseEntry();
            valueEntry.setPartial(true);
            return WALKey.decompose(txFpRawKeyValueEntryStream -> {
                if (cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                    do {
                        if (KeyUtil.compare(keyEntry.getData(), toFpPk) >= 0) {
                            return false;
                        }
                        if (!txFpRawKeyValueEntryStream.stream(-1, -1, null, keyEntry.getData(), false, null, -1, false, -1, null)) {
                            return false;
                        }
                    }
                    while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                }
                return true;
            }, (txId, fp, rowType, _prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
                long takeTxId = UIO.bytesLong(key, 0);
                long takeFp = UIO.bytesLong(key, 8);
                return txFpStream.stream(takeTxId, takeFp, hasValue, value);
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
            DatabaseEntry dbValue = new DatabaseEntry();
            byte[] pk = WALKey.compose(prefix, key);
            OperationStatus status = primaryDb.get(null, new DatabaseEntry(pk), dbValue, LockMode.READ_UNCOMMITTED);
            if (status == OperationStatus.SUCCESS) {
                return entryToWALPointer(prefix, key, dbValue.getData(), stream, true);
            } else {
                return stream.stream(prefix, key, -1, false, -1, -1, false, null);
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(byte[] prefix, UnprefixedWALKeys keys, WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dpPointerValue = new DatabaseEntry();

            return keys.consume((key) -> {
                dbKey.setData(WALKey.compose(prefix, key));
                OperationStatus status = primaryDb.get(null, dbKey, dpPointerValue, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    return entryToWALPointer(prefix, key, dpPointerValue.getData(), stream, true);
                } else {
                    return stream.stream(prefix, key, -1, false, -1, -1, false, null);
                }
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        lock.acquire();
        try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dpPointerValue = new DatabaseEntry();

            return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                byte[] pk = WALKey.compose(prefix, key);
                dbKey.setData(pk);
                OperationStatus status = primaryDb.get(null, dbKey, dpPointerValue, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    return entryToWALPointer(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, dpPointerValue.getData(), stream);
                } else {
                    return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion, -1, false, -1, -1, false, null);
                }
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
                (_prefix, _key, timestamp, tombstoned, version, fp, hasValue, value) -> {
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
        DiskOrderedCursor cursor = null;
        try {
            cursor = primaryDb.openCursor(new DiskOrderedCursorConfig().setKeysOnly(true).setQueueSize(1).setLSNBatchSize(1));
            DatabaseEntry value = new DatabaseEntry();
            value.setPartial(true);
            return (cursor.getNext(new DatabaseEntry(), value, LockMode.READ_UNCOMMITTED) != OperationStatus.SUCCESS);
        } finally {
            lock.release();
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public long deltaCount(WALKeyPointers keyPointers) throws Exception {
        lock.acquire();
        try {
            long[] delta = new long[1];
            boolean completed = keyPointers.consume((prefix, key, requestTimestamp, requestTombstoned, requestVersion, fp, hasValue, value) ->
                getPointer(prefix, key, (_prefix, _key, indexTimestamp, indexTombstoned, indexVersion, indexFp, indexHasValue, indexValue) -> {
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
//
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
    public void commit(boolean fsync) throws Exception {
        lock.acquire();
        try {
            environments[currentStripe].flushLog(false); // Hmm
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
    public boolean rowScan(final WALKeyPointerStream stream, boolean hydrateValues) throws Exception {
        lock.acquire();
        try (Cursor cursor = primaryDb.openCursor(null, null)) {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            return WALKey.decompose(
                (WALKey.TxFpRawKeyValueEntries<byte[]>) txFpRawKeyValueEntryStream -> {
                    while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                        if (!txFpRawKeyValueEntryStream.stream(-1, -1, null, keyEntry.getData(), false, null, -1, false, -1, valueEntry.getData())) {
                            return false;
                        }
                    }
                    return true;
                },
                (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
                    return entryToWALPointer(prefix, key, entry, stream, hydrateValues);
                });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, WALKeyPointerStream stream,
        boolean hydrateValues) throws Exception {
        lock.acquire();
        try (Cursor cursor = primaryDb.openCursor(null, null)) {
            byte[] fromPk = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
            byte[] toPk = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
            if (fromPk != null && toPk != null && KeyUtil.compare(fromPk, toPk) > 0) {
                // reverse scan
                DatabaseEntry keyEntry = new DatabaseEntry(toPk);
                DatabaseEntry valueEntry = new DatabaseEntry();
                return WALKey.decompose(
                    (WALKey.TxFpRawKeyValueEntries<byte[]>) txFpRawKeyValueEntryStream -> {
                        if (cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                            if (cursor.getPrev(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                                do {
                                    byte[] key = keyEntry.getData();
                                    if (KeyUtil.compare(key, fromPk) < 0) {
                                        return false;
                                    }
                                    byte[] entry = valueEntry.getData();
                                    if (!txFpRawKeyValueEntryStream.stream(-1, -1, null, key, false, null, -1, false, -1, entry)) {
                                        return false;
                                    }
                                }
                                while (cursor.getPrev(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                            }
                        }
                        return true;
                    },
                    (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
                        return entryToWALPointer(prefix, key, entry, stream, hydrateValues);
                    });
            } else {
                DatabaseEntry keyEntry = new DatabaseEntry(fromPk);
                DatabaseEntry valueEntry = new DatabaseEntry();
                return WALKey.decompose(
                    (WALKey.TxFpRawKeyValueEntries<byte[]>) txFpRawKeyValueEntryStream -> {
                        OperationStatus status;
                        if (fromPk == null) {
                            status = cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
                        } else {
                            status = cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED);
                        }
                        if (status == OperationStatus.SUCCESS) {
                            do {
                                if (toPk != null && KeyUtil.compare(keyEntry.getData(), toPk) >= 0) {
                                    return false;
                                }
                                if (!txFpRawKeyValueEntryStream.stream(-1, -1, null, keyEntry.getData(), false, null, -1, false, -1, valueEntry.getData())) {
                                    return false;
                                }
                            }
                            while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                        }
                        return true;
                    },
                    (txId, fp, rowType, prefix, key, hasValue, value, valueTimestamp, valueTombstoned, valueVersion, entry) -> {
                        return entryToWALPointer(prefix, key, entry, stream, hydrateValues);
                    });
            }
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

            final BerkeleyDBWALIndex compactingWALIndex = new BerkeleyDBWALIndex(providerName,
                versionedPartitionName,
                environments,
                name.typeName(Type.compacting),
                compactionStripe);

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
                        environments[compactionStripe].flushLog(fsync);
                        compactingWALIndex.close();
                        if (!compactingTo.compareAndSet(compactingWALIndex, null)) {
                            throw new IllegalStateException("Tried to commit a stale compaction index");
                        }
                        if (primaryDb == null || prefixDb == null) {
                            LOG.warn("Was not commited because index has been closed.");
                        } else {
                            LOG.debug("Committing before swap: {}", name.getPrimaryName());

                            renameDatabase(compactionStripe, Type.compacting, Type.compacted);

                            primaryDb.close();
                            primaryDb = null;
                            prefixDb.close();
                            prefixDb = null;
                            if (hasActive) {
                                renameDatabase(currentStripe, Type.active, Type.backup);
                            } else {
                                removeDatabase(currentStripe, Type.active);
                            }

                            if (commit != null) {
                                commit.call();
                            }

                            renameDatabase(compactionStripe, Type.compacted, Type.active);
                            removeDatabase(currentStripe, Type.backup);

                            primaryDb = environments[compactionStripe].openDatabase(null, name.getPrimaryName(), primaryDbConfig);
                            prefixDb = environments[compactionStripe].openDatabase(null, name.getPrefixName(), prefixDbConfig);
                            environments[compactionStripe].flushLog(true);
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

    private void renameDatabase(int stripe, Type fromType, Type toType) {
        environments[stripe].renameDatabase(null, name.typeName(fromType).getPrimaryName(), name.typeName(toType).getPrimaryName());
        environments[stripe].renameDatabase(null, name.typeName(fromType).getPrefixName(), name.typeName(toType).getPrefixName());
    }

    private void removeDatabase(int stripe, Type type) {
        try {
            environments[stripe].removeDatabase(null, name.typeName(type).getPrimaryName());
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
        try {
            environments[stripe].removeDatabase(null, name.typeName(type).getPrefixName());
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
    }

    @Override
    public void updatedProperties(Map<String, String> properties) {
    }

    @Override
    public int getStripe() {
        return currentStripe;
    }

}
