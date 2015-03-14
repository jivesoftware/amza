package com.jivesoftware.os.amza.mapdb;

import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowsIndex;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Semaphore;
import org.apache.commons.io.FileUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 *
 * @author jonathan.colt
 */
public class MapdbRowIndex implements RowsIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;
    private final Semaphore lock = new Semaphore(numPermits, true);

    private final TableName tableName;
    private final File dir;
    private DB db;
    private ConcurrentNavigableMap<RowIndexKey, RowIndexValue> index;

    public MapdbRowIndex(File dir, TableName tableName) {
        this.dir = dir;
        this.tableName = tableName;
        File active = new File(dir, "active");
        active.mkdirs();
        File tableFile = new File(active, "table");
        db = DBMaker.newFileDB(tableFile).mmapFileEnable().make();
        index = db.getTreeMap("table");
        db.commit();
        LOG.info("Opening " + active.getAbsolutePath() + " " + index.size());
    }

    @Override
    public void put(Collection<? extends Map.Entry<RowIndexKey, RowIndexValue>> entries) throws Exception {
        lock.acquire();
        try {
            for (Map.Entry<RowIndexKey, RowIndexValue> entry : entries) {
                index.put(entry.getKey(), entry.getValue());
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public List<RowIndexValue> get(List<RowIndexKey> keys) throws Exception {
        lock.acquire();
        try {
            List<RowIndexValue> gots = new ArrayList<>(keys.size());
            for (RowIndexKey key : keys) {
                gots.add(index.get(key));
            }
            return gots;

        } finally {
            lock.release();
        }
    }

    @Override
    public List<Boolean> containsKey(List<RowIndexKey> keys) throws Exception {
        lock.acquire();
        try {
            List<Boolean> contains = new ArrayList<>(keys.size());
            for (RowIndexKey key : keys) {
                contains.add(index.containsKey(key));
            }
            return contains;
        } finally {
            lock.release();
        }
    }

    @Override
    public void remove(Collection<RowIndexKey> keys) throws Exception {
        lock.acquire();
        try {
            for (RowIndexKey key : keys) {
                index.remove(key);
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        lock.acquire();
        try {
            return index.isEmpty();
        } finally {
            lock.release();
        }
    }

    @Override
    public void clear() throws Exception {

    }

    @Override
    public void commit() {

    }

    @Override
    public void compact() {

    }

    public void close() throws Exception {
        lock.acquire(numPermits);
        try {
            db.commit();
            db.close();
            db = null;
            index = null;
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public <E extends Exception> void rowScan(final RowScan<E> rowScan) throws Exception {
        lock.acquire();
        try {
            for (Map.Entry<RowIndexKey, RowIndexValue> e : index.entrySet()) {
                RowIndexKey key = e.getKey();
                RowIndexValue value = e.getValue();
                if (!rowScan.row(-1, key, value)) {
                    break;
                }
            }
        } finally {
            lock.release();
        }
    }

    @Override
    synchronized public <E extends Exception> void rangeScan(RowIndexKey from, RowIndexKey to, final RowScan<E> rowScan) throws Exception {
        lock.acquire();
        try {

            for (Map.Entry<RowIndexKey, RowIndexValue> e : index.subMap(from, to).entrySet()) {
                RowIndexKey key = e.getKey();
                RowIndexValue value = e.getValue();
                if (!rowScan.row(-1, key, value)) {
                    break;
                }
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public CompactionRowIndex startCompaction() throws Exception {
        final File compacting = new File(dir, "compacting");
        FileUtils.deleteDirectory(compacting);

        final File compacted = new File(dir, "compacted");
        FileUtils.deleteDirectory(compacted);

        final File backup = new File(dir, "backup");
        FileUtils.deleteDirectory(backup);

        compacting.mkdirs();
        final MapdbRowIndex compactedRowIndex = new MapdbRowIndex(compacting, tableName);

        return new CompactionRowIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<RowIndexKey, RowIndexValue>> entries) throws Exception {
                compactedRowIndex.put(entries);
            }

            @Override
            public void abort() throws Exception {
                try {
                    compactedRowIndex.close();
                } catch (IOException ex) {
                    throw new RuntimeException();
                }
            }

            @Override
            public void commit() throws Exception {
                lock.acquire(numPermits);
                try {

                    LOG.info("Commiting before swap." + new File(dir, "active") + " " + index.size());
                    db.commit();
                    db.close();
                    db = null;
                    index = null;

                    FileUtils.moveDirectory(new File(compacting, "active"), compacted);
                    FileUtils.moveDirectory(new File(dir, "active"), backup);
                    FileUtils.moveDirectory(compacted, new File(dir, "active"));
                    FileUtils.deleteDirectory(backup);

                    db = compactedRowIndex.db;
                    index = compactedRowIndex.index;
                    db.commit();
                    LOG.info("Commiting after swap." + new File(dir, "active") + " " + index.size());
                } finally {
                    lock.release(numPermits);
                }
            }
        };
    }
}
