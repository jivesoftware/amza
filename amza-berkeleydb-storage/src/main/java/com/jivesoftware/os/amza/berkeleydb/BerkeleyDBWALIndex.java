package com.jivesoftware.os.amza.berkeleydb;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexName.Type;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.wal.KeyUtil;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValuePointerStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointers;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeys;
import com.jivesoftware.os.amza.shared.wal.WALMergeKeyPointerStream;
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
import java.io.IOException;
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

    private final Environment environment;
    private final BerkeleyDBWALIndexName name;
    private final DatabaseConfig primaryDbConfig;
    private final DatabaseConfig prefixDbConfig;
    private Database primaryDb;
    private Database prefixDb;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);
    private final AtomicReference<WALIndex> compactingTo = new AtomicReference<>();

    public BerkeleyDBWALIndex(Environment environment, BerkeleyDBWALIndexName name) throws Exception {
        this.environment = environment;
        this.name = name;

        // Open the database, creating one if it does not exist
        this.primaryDbConfig = new DatabaseConfig()
            .setAllowCreate(true)
            .setBtreeComparator(KeyUtil.lexicographicalComparator());
        this.primaryDb = environment.openDatabase(null, name.getPrimaryName(), primaryDbConfig);

        // Open the database, creating one if it does not exist
        this.prefixDbConfig = new DatabaseConfig()
            .setAllowCreate(true)
            .setBtreeComparator(UnsignedBytes.lexicographicalComparator());
        this.prefixDb = environment.openDatabase(null, name.getPrefixName(), prefixDbConfig);
    }

    private void walPointerToEntry(long fp, long timestamp, boolean tombstoned, DatabaseEntry dbValue) {
        byte[] valueBytes = UIO.longBytes(fp);
        byte[] entryBytes = new byte[valueBytes.length + 8 + 1];
        System.arraycopy(valueBytes, 0, entryBytes, 0, valueBytes.length);
        UIO.longBytes(timestamp, entryBytes, valueBytes.length);
        entryBytes[valueBytes.length + 8] = tombstoned ? (byte) 1 : (byte) 0;
        dbValue.setData(entryBytes);
    }

    private boolean entryToWALPointer(byte[] prefix, byte[] key, byte[] entryBytes, WALKeyPointerStream stream) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        return stream.stream(prefix, key, timestamp, tombstoned, UIO.bytesLong(valueBytes));
    }

    private boolean entryToWALPointer(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned,
        byte[] entryBytes, KeyValuePointerStream stream) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, timestamp, tombstoned, UIO.bytesLong(valueBytes));
    }

    private long entryToTimestamp(byte[] entryBytes) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        return UIO.bytesLong(entryBytes, valueBytes.length);
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
            DatabaseEntry dbValue = new DatabaseEntry();
            return pointers.consume((txId, prefix, key, timestamp, tombstoned, fp) -> {
                byte[] pk = WALKey.compose(prefix, key);
                OperationStatus status = primaryDb.get(null, new DatabaseEntry(pk), dbValue, LockMode.READ_UNCOMMITTED);
                byte mode;
                if (status == OperationStatus.SUCCESS) {
                    mode = (entryToTimestamp(dbValue.getData()) < timestamp) ? WALMergeKeyPointerStream.clobbered : WALMergeKeyPointerStream.ignored;
                } else {
                    mode = WALMergeKeyPointerStream.added;
                }
                if (mode != WALMergeKeyPointerStream.ignored) {
                    walPointerToEntry(fp, timestamp, tombstoned, dbValue);
                    primaryDb.put(null, new DatabaseEntry(pk), dbValue);
                }
                if (stream != null) {
                    return stream.stream(mode, txId, prefix, key, timestamp, tombstoned, fp);
                } else {
                    return true;
                }
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
                return entryToWALPointer(prefix, key, dbValue.getData(), stream);
            } else {
                return stream.stream(prefix, key, -1, false, -1);
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dpPointerValue = new DatabaseEntry();

            return keys.consume((byte[] prefix, byte[] key) -> {
                dbKey.setData(WALKey.compose(prefix, key));
                OperationStatus status = primaryDb.get(null, dbKey, dpPointerValue, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    return entryToWALPointer(prefix, key, dpPointerValue.getData(), stream);
                } else {
                    return stream.stream(prefix, key, -1, false, -1);
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

            return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned) -> {
                byte[] pk = WALKey.compose(prefix, key);
                dbKey.setData(pk);
                OperationStatus status = primaryDb.get(null, dbKey, dpPointerValue, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    return entryToWALPointer(prefix, key, value, valueTimestamp, valueTombstoned, dpPointerValue.getData(), stream);
                } else {
                    return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, -1, false, -1);
                }
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        lock.acquire();
        try {
            return keys.consume((prefix, key) -> getPointer(prefix, key,
                (_prefix, _key, timestamp, tombstoned, fp) -> {
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
    public long size() throws Exception {
        lock.acquire();
        try {
            long size = count.get();
            if (size >= 0) {
                return size;
            }
            int numCommits = commits.get();
            size = primaryDb.count();
            synchronized (commits) {
                if (numCommits == commits.get()) {
                    count.set(size);
                }
            }
            return size;
        } finally {
            lock.release();
        }
    }

    @Override
    public void commit() throws Exception {
        lock.acquire();
        try {
            environment.flushLog(false);
            synchronized (commits) {
                count.set(-1);
                commits.incrementAndGet();
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public void compact() {
    }

    @Override
    public void close() throws Exception {
        lock.acquire(numPermits);
        try {
            primaryDb.close();
            primaryDb = null;
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public boolean rowScan(final WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        try (Cursor cursor = primaryDb.openCursor(null, null)) {
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            return WALKey.decompose((WALKey.TxFpRawKeyValueEntries<byte[]>) txFpRawKeyValueEntryStream -> {
                while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                    if (!txFpRawKeyValueEntryStream.stream(-1, -1, keyEntry.getData(), null, -1, false, valueEntry.getData())) {
                        return false;
                    }
                }
                return true;
            }, (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry) -> entryToWALPointer(prefix, key, entry, stream));
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        try (Cursor cursor = primaryDb.openCursor(null, null)) {
            byte[] fromPk = WALKey.compose(fromPrefix, fromKey);
            byte[] toPk = WALKey.compose(toPrefix, toKey);
            DatabaseEntry keyEntry = new DatabaseEntry(fromPk);
            DatabaseEntry valueEntry = new DatabaseEntry();
            return WALKey.decompose((WALKey.TxFpRawKeyValueEntries<byte[]>) txFpRawKeyValueEntryStream -> {
                do {
                    if (toPk != null && WALKey.compare(keyEntry.getData(), toPk) >= 0) {
                        return false;
                    }
                    if (!txFpRawKeyValueEntryStream.stream(-1, -1, keyEntry.getData(), null, -1, false, valueEntry.getData())) {
                        return false;
                    }
                }
                while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
                return true;
            }, (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry) -> entryToWALPointer(prefix, key, entry, stream));
        } finally {
            lock.release();
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {

        synchronized (compactingTo) {
            WALIndex got = compactingTo.get();
            if (got != null) {
                throw new IllegalStateException("Try to compact while another compaction is already underway: " + name);
            }

            if (primaryDb == null) {
                throw new IllegalStateException("Try to compact a index that has been expunged: " + name);
            }

            removeDatabase(Type.compacting);
            removeDatabase(Type.compacted);
            removeDatabase(Type.backup);

            final BerkeleyDBWALIndex compactingWALIndex = new BerkeleyDBWALIndex(environment, name.typeName(Type.compacting));
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
                        if (primaryDb == null) {
                            LOG.warn("Was not commited because index has been closed.");
                        } else {
                            LOG.info("Committing before swap: {}", name.getPrimaryName());

                            compactingWALIndex.close();
                            renameDatabase(Type.compacting, Type.compacted);

                            primaryDb.close();
                            primaryDb = null;
                            renameDatabase(Type.active, Type.backup);

                            renameDatabase(Type.compacted, Type.active);
                            removeDatabase(Type.backup);

                            primaryDb = environment.openDatabase(null, name.getPrimaryName(), primaryDbConfig);

                            LOG.info("Committing after swap: {}", name.getPrimaryName());
                        }
                    } finally {
                        lock.release(numPermits);
                    }
                }
            };
        }
    }

    private void renameDatabase(Type fromType, Type toType) {
        environment.renameDatabase(null, name.typeName(fromType).getPrimaryName(), name.typeName(toType).getPrimaryName());
    }

    private void removeDatabase(Type type) {
        try {
            environment.removeDatabase(null, name.typeName(type).getPrimaryName());
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

}
