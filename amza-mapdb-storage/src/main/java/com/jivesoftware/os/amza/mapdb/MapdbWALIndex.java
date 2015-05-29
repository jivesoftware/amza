package com.jivesoftware.os.amza.mapdb;

import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
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
 * @author jonathan.colt
 */
public class MapdbWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final int numPermits = 1024;
    private final Semaphore lock = new Semaphore(numPermits, true);

    private final VersionedRegionName versionedRegionName;
    private final File dir;
    private DB db;
    private ConcurrentNavigableMap<WALKey, WALPointer> index;

    public MapdbWALIndex(File dir, VersionedRegionName versionedRegionName) {
        this.dir = dir;
        this.versionedRegionName = versionedRegionName;
        File active = new File(dir, "active");
        active.mkdirs();
        File regionFile = new File(active, "region");
        db = DBMaker.newFileDB(regionFile).cacheSoftRefEnable().mmapFileEnable().closeOnJvmShutdown().make();
        index = db.getTreeMap("region");
        db.commit();
        LOG.info("Opening " + active.getAbsolutePath() + " " + index.size());
    }

    @Override
    public void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entries) throws Exception {
        lock.acquire();
        try {
            for (Map.Entry<WALKey, WALPointer> entry : entries) {
                index.put(entry.getKey(), entry.getValue());
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public WALPointer getPointer(WALKey key) throws Exception {
        lock.acquire();
        try {
            return index.get(key);
        } finally {
            lock.release();
        }
    }

    @Override
    public WALPointer[] getPointers(WALKey[] consumableKeys) throws Exception {
        lock.acquire();
        try {
            WALPointer[] gots = new WALPointer[consumableKeys.length];
            for (int i = 0; i < consumableKeys.length; i++) {
                if (consumableKeys[i] != null) {
                    gots[i] = index.get(consumableKeys[i]);
                    if (gots[i] != null) {
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
            for (WALKey key : keys) {
                WALPointer got = index.get(key);
                contains.add(got == null ? false : !got.getTombstoned());
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
    public long size() throws Exception {
        lock.acquire();
        try {
            return index.size();
        } finally {
            lock.release();
        }
    }

    @Override
    public void commit() throws Exception {
        lock.acquire();
        try {
            db.commit();
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
            db.commit();
            db.close();
            db = null;
            index = null;
        } finally {
            lock.release(numPermits);
        }
    }

    @Override
    public void rowScan(final Scan<WALPointer> scan) throws Exception {
        lock.acquire();
        try {
            for (Map.Entry<WALKey, WALPointer> e : index.entrySet()) {
                WALKey key = e.getKey();
                WALPointer rowPointer = e.getValue();
                if (!scan.row(-1, key, rowPointer)) {
                    break;
                }
            }
        } finally {
            lock.release();
        }
    }

    @Override
    synchronized public void rangeScan(WALKey from, WALKey to, final Scan<WALPointer> scan) throws Exception {
        lock.acquire();
        try {

            for (Map.Entry<WALKey, WALPointer> e : index.subMap(from, to).entrySet()) {
                WALKey key = e.getKey();
                WALPointer rowPointer = e.getValue();
                if (!scan.row(-1, key, rowPointer)) {
                    break;
                }
            }
        } finally {
            lock.release();
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        final File compacting = new File(dir, "compacting");
        FileUtils.deleteDirectory(compacting);

        final File compacted = new File(dir, "compacted");
        FileUtils.deleteDirectory(compacted);

        final File backup = new File(dir, "backup");
        FileUtils.deleteDirectory(backup);

        compacting.mkdirs();
        final MapdbWALIndex compactedRowIndex = new MapdbWALIndex(compacting, versionedRegionName);

        return new CompactionWALIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entries) throws Exception {
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

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {

    }

    @Override
    public boolean delete() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); 
    }
}
