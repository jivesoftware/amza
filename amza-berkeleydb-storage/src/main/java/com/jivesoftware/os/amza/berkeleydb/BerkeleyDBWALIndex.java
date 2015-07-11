package com.jivesoftware.os.amza.berkeleydb;

import com.jivesoftware.os.amza.berkeleydb.BerkeleyDBWALIndexName.Prefix;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.wal.KeyContainedStream;
import com.jivesoftware.os.amza.shared.wal.KeyValues;
import com.jivesoftware.os.amza.shared.wal.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.TxKeyPointers;
import com.jivesoftware.os.amza.shared.wal.WALIndex;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyValuePointerStream;
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
    private final DatabaseConfig dbConfig;
    private Database database;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);
    private final AtomicReference<WALIndex> compactingTo = new AtomicReference<>();

    public BerkeleyDBWALIndex(Environment environment, BerkeleyDBWALIndexName name) throws Exception {
        this.environment = environment;
        this.name = name;

        // Open the database, creating one if it does not exist
        this.dbConfig = new DatabaseConfig()
            .setAllowCreate(true);
        this.database = environment.openDatabase(null, name.getName(), dbConfig);
    }

    private void walPointerToEntry(long fp, long timestamp, boolean tombstoned, DatabaseEntry dbValue) {
        byte[] valueBytes = UIO.longBytes(fp);
        byte[] entryBytes = new byte[valueBytes.length + 8 + 1];
        System.arraycopy(valueBytes, 0, entryBytes, 0, valueBytes.length);
        UIO.longBytes(timestamp, entryBytes, valueBytes.length);
        entryBytes[valueBytes.length + 8] = tombstoned ? (byte) 1 : (byte) 0;
        dbValue.setData(entryBytes);
    }

    private boolean entryToWALPointer(byte[] key, byte[] entryBytes, WALKeyPointerStream stream) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        return stream.stream(key, timestamp, tombstoned, UIO.bytesLong(valueBytes));
    }

    private boolean entryToWALPointer(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned,
        byte[] entryBytes, WALKeyValuePointerStream stream) throws Exception {
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        return stream.stream(key, value, valueTimestamp, valueTombstoned, timestamp, tombstoned, UIO.bytesLong(valueBytes));
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
                removeDatabase(Prefix.active);
                removeDatabase(Prefix.backup);
                removeDatabase(Prefix.compacted);
                removeDatabase(Prefix.compacting);
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
            return pointers.consume((txId, key, timestamp, tombstoned, fp) -> {
                OperationStatus status = database.get(null, new DatabaseEntry(key), dbValue, LockMode.READ_UNCOMMITTED);
                byte mode;
                if (status == OperationStatus.SUCCESS) {
                    mode = (entryToTimestamp(dbValue.getData()) < timestamp) ? WALMergeKeyPointerStream.clobbered : WALMergeKeyPointerStream.ignored;
                } else {
                    mode = WALMergeKeyPointerStream.added;
                }
                if (mode != WALMergeKeyPointerStream.ignored) {
                    walPointerToEntry(fp, timestamp, tombstoned, dbValue);
                    database.put(null, new DatabaseEntry(key), dbValue);
                }
                if (stream != null) {
                    return stream.stream(mode, txId, key, timestamp, tombstoned, fp);
                } else {
                    return true;
                }
            });

        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        try {
            lock.acquire();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
        try {
            DatabaseEntry dbValue = new DatabaseEntry();
            OperationStatus status = database.get(null, new DatabaseEntry(key), dbValue, LockMode.READ_UNCOMMITTED);
            if (status == OperationStatus.SUCCESS) {
                return entryToWALPointer(key, dbValue.getData(), stream);
            } else {
                return stream.stream(key, -1, false, -1);
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

            return keys.consume((byte[] key) -> {
                dbKey.setData(key);
                OperationStatus status = database.get(null, dbKey, dpPointerValue, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    return entryToWALPointer(key, dpPointerValue.getData(), stream);
                } else {
                    return stream.stream(key, -1, false, -1);
                }
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        lock.acquire();
        try {
            DatabaseEntry dbKey = new DatabaseEntry();
            DatabaseEntry dpPointerValue = new DatabaseEntry();

            return keyValues.consume((byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) -> {
                dbKey.setData(key);
                OperationStatus status = database.get(null, dbKey, dpPointerValue, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    return entryToWALPointer(key, value, valueTimestamp, valueTombstoned, dpPointerValue.getData(), stream);
                } else {
                    return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
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
            return keys.consume(key -> getPointer(key,
                (_key, timestamp, tombstoned, fp) -> {
                    stream.stream(key, fp != -1 && !tombstoned);
                    return true;
                }));
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean remove(WALKeys keys) throws Exception {
        lock.acquire();
        try {
            return keys.consume(key -> {
                database.delete(null, new DatabaseEntry(key));
                return true;
            });
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        lock.acquire();
        DiskOrderedCursor cursor = null;
        try {
            cursor = database.openCursor(null);
            return (cursor.getNext(new DatabaseEntry(), new DatabaseEntry(), LockMode.READ_UNCOMMITTED) != OperationStatus.SUCCESS);
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
            size = database.count();
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
            database.close();
            database = null;
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public boolean rowScan(final WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        Cursor cursor = null;
        try {
            cursor = database.openCursor(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                if (!entryToWALPointer(keyEntry.getData(), valueEntry.getData(), stream)) {
                    return false;
                }
            }
            return true;
        } finally {
            lock.release();
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        lock.acquire();
        Cursor cursor = null;
        try {
            cursor = database.openCursor(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(from);
            DatabaseEntry valueEntry = new DatabaseEntry();
            if (cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                do {
                    if (to != null && WALKey.compare(keyEntry.getData(), to) >= 0) {
                        return false;
                    }
                    if (!entryToWALPointer(keyEntry.getData(), valueEntry.getData(), stream)) {
                        return false;
                    }
                }
                while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
            }
            return true;
        } finally {
            lock.release();
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {

        synchronized (compactingTo) {
            WALIndex got = compactingTo.get();
            if (got != null) {
                throw new IllegalStateException("Try to compact while another compaction is already underway: " + name);
            }

            if (database == null) {
                throw new IllegalStateException("Try to compact a index that has been expunged: " + name);
            }

            removeDatabase(Prefix.compacting);
            removeDatabase(Prefix.compacted);
            removeDatabase(Prefix.backup);

            final BerkeleyDBWALIndex compactingWALIndex = new BerkeleyDBWALIndex(environment, name.prefixName(BerkeleyDBWALIndexName.Prefix.compacting));
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
                        if (database == null) {
                            LOG.warn("Was not commited because index has been closed.");
                        } else {
                            LOG.info("Committing before swap: {}", name.getName());

                            compactingWALIndex.close();
                            renameDatabase(Prefix.compacting, Prefix.compacted);

                            database.close();
                            database = null;
                            renameDatabase(Prefix.active, Prefix.backup);

                            renameDatabase(Prefix.compacted, Prefix.active);
                            removeDatabase(Prefix.backup);

                            database = environment.openDatabase(null, name.getName(), dbConfig);

                            LOG.info("Committing after swap: {}", name.getName());
                        }
                    } finally {
                        lock.release(numPermits);
                    }
                }
            };
        }
    }

    private void renameDatabase(Prefix fromPrefix, Prefix toPrefix) {
        environment.renameDatabase(null, name.prefixName(fromPrefix).getName(), name.prefixName(toPrefix).getName());
    }

    private void removeDatabase(Prefix prefix) {
        try {
            environment.removeDatabase(null, name.prefixName(prefix).getName());
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

}
