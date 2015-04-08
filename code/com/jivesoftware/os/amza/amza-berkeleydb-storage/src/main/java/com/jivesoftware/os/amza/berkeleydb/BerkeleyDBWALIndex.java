package com.jivesoftware.os.amza.berkeleydb;

import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.amza.shared.filer.UIO;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class BerkeleyDBWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;

    private final Environment environment;
    private final String name;
    private final DatabaseConfig dbConfig;
    private Database database;

    private final Semaphore lock = new Semaphore(numPermits, true);
    private final AtomicLong count = new AtomicLong(-1);
    private final AtomicInteger commits = new AtomicInteger(0);

    public BerkeleyDBWALIndex(Environment environment, String prefix, String name) throws Exception {
        this.environment = environment;
        this.name = name;

        // Open the database, creating one if it does not exist
        this.dbConfig = new DatabaseConfig()
            .setAllowCreate(true);
        this.database = environment.openDatabase(null, getDatabaseName(prefix), dbConfig);
    }

    private String getDatabaseName(String prefix) {
        return prefix + "-" + name;
    }

    private DatabaseEntry walPointerToEntry(WALPointer rowPointer) {
        byte[] valueBytes = UIO.longBytes(rowPointer.getFp());
        long timestamp = rowPointer.getTimestampId();
        boolean tombstoned = rowPointer.getTombstoned();
        byte[] entryBytes = new byte[valueBytes.length + 8 + 1];
        System.arraycopy(valueBytes, 0, entryBytes, 0, valueBytes.length);
        UIO.longBytes(timestamp, entryBytes, valueBytes.length);
        entryBytes[valueBytes.length + 8] = tombstoned ? (byte) 1 : (byte) 0;
        return new DatabaseEntry(entryBytes);
    }

    private WALPointer entryToWALPointer(DatabaseEntry entry) {
        byte[] entryBytes = entry.getData();
        byte[] valueBytes = new byte[entryBytes.length - 8 - 1];
        System.arraycopy(entryBytes, 0, valueBytes, 0, valueBytes.length);
        long timestamp = UIO.bytesLong(entryBytes, valueBytes.length);
        boolean tombstoned = (entryBytes[valueBytes.length + 8] == (byte) 1);
        return new WALPointer(UIO.bytesLong(valueBytes), timestamp, tombstoned);
    }

    @Override
    public void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entries) throws Exception {
        lock.acquire();
        try {
            for (Map.Entry<WALKey, WALPointer> entry : entries) {
                database.put(null, new DatabaseEntry(entry.getKey().getKey()), walPointerToEntry(entry.getValue()));
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public WALPointer getPointer(WALKey key) throws Exception {
        lock.acquire();
        try {
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus status = database.get(null, new DatabaseEntry(key.getKey()), value, LockMode.READ_UNCOMMITTED);
            if (status == OperationStatus.SUCCESS) {
                return entryToWALPointer(value);
            }
            return null;
        } finally {
            lock.release();
        }
    }

    @Override
    public WALPointer[] getPointers(WALKey[] consumableKeys) throws Exception {
        lock.acquire();
        try {
            WALPointer[] gots = new WALPointer[consumableKeys.length];
            DatabaseEntry value = new DatabaseEntry();
            for (int i = 0; i < consumableKeys.length; i++) {
                WALKey key = consumableKeys[i];
                if (consumableKeys[i] != null) {
                    OperationStatus status = database.get(null, new DatabaseEntry(key.getKey()), value, LockMode.READ_UNCOMMITTED);
                    if (status == OperationStatus.SUCCESS) {
                        gots[i] = entryToWALPointer(value);
                        consumableKeys[i] = null;
                    }
                }
            }
            return gots;
        } finally {
            lock.release();
        }
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> keys) throws Exception {
        lock.acquire();
        try {
            List<Boolean> contains = new ArrayList<>(keys.size());
            DatabaseEntry value = new DatabaseEntry();
            for (WALKey key : keys) {
                OperationStatus status = database.get(null, new DatabaseEntry(key.getKey()), value, LockMode.READ_UNCOMMITTED);
                if (status == OperationStatus.SUCCESS) {
                    contains.add(true);
                } else {
                    contains.add(false);
                }
            }
            return contains;
        } finally {
            lock.release();
        }
    }

    @Override
    public void remove(Collection<WALKey> keys) throws Exception {
        lock.acquire();
        try {
            for (WALKey key : keys) {
                database.delete(null, new DatabaseEntry(key.getKey()));
            }
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
    public void rowScan(final Scan<WALPointer> scan) throws Exception {
        lock.acquire();
        Cursor cursor = null;
        try {
            cursor = database.openCursor(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry valueEntry = new DatabaseEntry();
            while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                WALKey key = new WALKey(keyEntry.getData());
                WALPointer rowPointer = entryToWALPointer(valueEntry);
                if (!scan.row(-1, key, rowPointer)) {
                    break;
                }
            }
        } finally {
            lock.release();
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    synchronized public void rangeScan(WALKey from, WALKey to, final Scan<WALPointer> scan) throws Exception {
        lock.acquire();
        Cursor cursor = null;
        try {
            cursor = database.openCursor(null, null);
            DatabaseEntry keyEntry = new DatabaseEntry(from.getKey());
            DatabaseEntry valueEntry = new DatabaseEntry();
            if (cursor.getSearchKeyRange(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                do {
                    WALKey key = new WALKey(keyEntry.getData());
                    if (key.compareTo(to) >= 0) {
                        break;
                    }
                    WALPointer value = entryToWALPointer(valueEntry);
                    if (!scan.row(-1, key, value)) {
                        break;
                    }
                }
                while (cursor.getNext(keyEntry, valueEntry, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS);
            }
        } finally {
            lock.release();
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        removeDatabase("compacting");
        removeDatabase("compacted");
        removeDatabase("backup");

        final BerkeleyDBWALIndex compactingWALIndex = new BerkeleyDBWALIndex(environment, "compacting", name);

        return new CompactionWALIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entries) throws Exception {
                compactingWALIndex.put(entries);
            }

            @Override
            public void abort() throws Exception {
                try {
                    compactingWALIndex.close();
                } catch (IOException ex) {
                    throw new RuntimeException();
                }
            }

            @Override
            public void commit() throws Exception {
                lock.acquire(numPermits);
                try {
                    LOG.info("Committing before swap: {}", getDatabaseName("active"));

                    compactingWALIndex.close();
                    renameDatabase("compacting", "compacted");

                    database.close();
                    database = null;
                    renameDatabase("active", "backup");

                    renameDatabase("compacted", "active");
                    removeDatabase("backup");

                    database = environment.openDatabase(null, getDatabaseName("active"), dbConfig);

                    LOG.info("Committing after swap: {}", getDatabaseName("active"));
                } finally {
                    lock.release(numPermits);
                }
            }
        };
    }

    private void renameDatabase(String fromPrefix, String toPrefix) {
        environment.renameDatabase(null, getDatabaseName(fromPrefix), getDatabaseName(toPrefix));
    }

    private void removeDatabase(String prefix) {
        try {
            environment.removeDatabase(null, getDatabaseName(prefix));
        } catch (DatabaseNotFoundException e) {
            // yummm
        }
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }
}
