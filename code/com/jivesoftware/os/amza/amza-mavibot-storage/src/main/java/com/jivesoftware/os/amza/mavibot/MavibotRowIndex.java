package com.jivesoftware.os.amza.mavibot;

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
import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.RecordManager;
import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.TupleCursor;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;

/**
 *
 * @author jonathan.colt
 */
public class MavibotRowIndex implements RowsIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TableName tableName;
    private final File dir;
    private RecordManager recordManager;
    private BTree<RowIndexKey, RowIndexValue> index;

    public MavibotRowIndex(File dir, TableName tableName) throws IOException, BTreeAlreadyManagedException {
        this.dir = dir;
        this.tableName = tableName;

        //TODO handle startup case realted to partial compaction
        File table = new File(dir, "active");
        table.mkdirs();
        this.recordManager = new RecordManager(table.getAbsolutePath());
        String name = tableName.getTableName() + "-" + tableName.getRingName();
        BTree<RowIndexKey, RowIndexValue> got = this.recordManager.getManagedTree(name);
        if (got == null) {
            got = this.recordManager.addBTree(name, new MaviBotRowIndexKeySerializer(), new MaviBotRowIndexValueSerializer(), false);
        }
        this.index = got;
    }

    @Override
    public void put(Collection<? extends Map.Entry<RowIndexKey, RowIndexValue>> entries) {
        try {
            for (Map.Entry<RowIndexKey, RowIndexValue> entry : entries) {
                index.insert(entry.getKey(), entry.getValue());
            }
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public List<RowIndexValue> get(List<RowIndexKey> keys) {
        try {
            List<RowIndexValue> gots = new ArrayList<>(keys.size());
            for (RowIndexKey key : keys) {
                try {
                    gots.add(index.get(key));
                } catch (KeyNotFoundException x) { // LAME!!
                    gots.add(null);
                }
            }
            return gots;

        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public List<Boolean> containsKey(List<RowIndexKey> keys) {
        try {
            List<Boolean> contains = new ArrayList<>(keys.size());
            for (RowIndexKey key : keys) {
                contains.add(index.hasKey(key));
            }
            return contains;
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public void remove(Collection<RowIndexKey> keys) {
        try {
            for (RowIndexKey key : keys) {
                index.delete(key);
            }
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public boolean isEmpty() {
        try {
            return index.getNbElems() > 0;
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public void clear() {

    }

    @Override
    synchronized public void commit() {

    }

    @Override
    synchronized public void compact() {

    }

    synchronized public void close() throws IOException {
        index.close();
        recordManager.close();
    }

    @Override
    synchronized public <E extends Exception> void rowScan(final RowScan<E> rowScan) throws E {
        try {

            TupleCursor<RowIndexKey, RowIndexValue> browse = index.browse();
            while (browse.hasNext()) {
                Tuple<RowIndexKey, RowIndexValue> next = browse.next();
                if (!rowScan.row(-1, next.getKey(), next.getValue())) {
                    break;
                }
            }
        } catch (Exception x) {
            throw new RuntimeException("streamKeys failure:", x);
        }
    }

    @Override
    synchronized public <E extends Exception> void rangeScan(RowIndexKey from, RowIndexKey to, final RowScan<E> rowScan) throws E {
        try {
            TupleCursor<RowIndexKey, RowIndexValue> browse = index.browseFrom(from);
            while (browse.hasNext()) {
                Tuple<RowIndexKey, RowIndexValue> next = browse.next();
                if (next.getKey().compareTo(to) >= 0) {
                    break;
                }
                if (!rowScan.row(-1, next.getKey(), next.getValue())) {
                    break;
                }
            }
        } catch (Exception x) {
            throw new RuntimeException("streamKeys failure:", x);
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
        final MavibotRowIndex fileBackedRowIndex = new MavibotRowIndex(compacting, tableName);

        return new CompactionRowIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<RowIndexKey, RowIndexValue>> entries) {
                fileBackedRowIndex.put(entries);
            }

            @Override
            public void abort() {
                try {
                    fileBackedRowIndex.close();
                } catch (IOException ex) {
                    throw new RuntimeException();
                }
            }

            @Override
            public void commit() throws Exception {
                synchronized (MavibotRowIndex.this) {
                    index.flush();
                    index.close();
                    recordManager.close();

                    FileUtils.moveDirectory(new File(compacting, "active"), compacted);
                    FileUtils.moveDirectory(new File(dir, "active"), backup);
                    FileUtils.moveDirectory(compacted, new File(dir, "active"));
                    FileUtils.deleteDirectory(backup);

                    index = fileBackedRowIndex.index;
                    recordManager = fileBackedRowIndex.recordManager;
                }
            }
        };
    }
}
