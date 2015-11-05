package com.jivesoftware.os.amza.lsm;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.api.Pointers;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author jonathan.colt
 */
public class LSMPointerIndex implements PointerIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final File indexRoot;
    private final int maxUpdatesBetweenCompactionHintMarker;
    private MemoryPointerIndex memoryPointerIndex;
    private MemoryPointerIndex flushingMemoryPointerIndex;
    private final AtomicLong largestIndexId = new AtomicLong();
    private final MergeablePointerIndexs mergeablePointerIndexs;

    public LSMPointerIndex(File indexRoot, int maxUpdatesBetweenCompactionHintMarker) throws IOException {
        this.indexRoot = indexRoot;
        this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
        this.memoryPointerIndex = new MemoryPointerIndex();
        this.mergeablePointerIndexs = new MergeablePointerIndexs();
        TreeSet<Long> indexIds = new TreeSet<>((java.lang.Long o1, java.lang.Long o2) -> Long.compare(o2, o1)); // descending
        for (File indexFile : indexRoot.listFiles()) {
            long indexId = Long.parseLong(FilenameUtils.removeExtension(indexFile.getName()));
            indexIds.add(indexId);
            if (largestIndexId.get() < indexId) {
                largestIndexId.set(indexId);
            }
        }
        for (Long indexId : indexIds) {
            File index = new File(indexRoot, String.valueOf(indexId));
            DiskBackedPointerIndex pointerIndex = new DiskBackedPointerIndex(new DiskBackedPointerIndexFiler(new File(index, "index").getAbsolutePath(), "rw",
                false), new DiskBackedPointerIndexFiler(new File(index, "keys").getAbsolutePath(), "rw", false));
            mergeablePointerIndexs.append(pointerIndex);
        }
    } // descending

    private ConcurrentReadablePointerIndex[] grab() {
        MemoryPointerIndex stackCopy = flushingMemoryPointerIndex;
        int flushing = stackCopy == null ? 0 : 1;
        ConcurrentReadablePointerIndex[] grabbed = mergeablePointerIndexs.grab();
        ConcurrentReadablePointerIndex[] indexes = new ConcurrentReadablePointerIndex[grabbed.length + 1 + flushing];
        synchronized (this) {
            indexes[0] = memoryPointerIndex;
            if (stackCopy != null) {
                indexes[1] = stackCopy;
            }
        }
        System.arraycopy(grabbed, 0, indexes, 1 + flushing, grabbed.length);
        return indexes;
    }

    @Override
    public NextPointer getPointer(byte[] key) throws Exception {
        return PointerIndexUtil.get(grab(), key);
    }

    @Override
    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        return PointerIndexUtil.rangeScan(grab(), from, to);
    }

    @Override
    public NextPointer rowScan() throws Exception {
        return PointerIndexUtil.rowScan(grab());
    }

    @Override
    public void close() throws Exception {
        memoryPointerIndex.close();
        mergeablePointerIndexs.close();
    }

    @Override
    public long count() throws Exception {
        return memoryPointerIndex.count() + mergeablePointerIndexs.count();
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (memoryPointerIndex.isEmpty()) {
            return mergeablePointerIndexs.isEmpty();
        }
        return false;
    }

    @Override
    public void append(Pointers pointers) throws Exception {
        long[] count = {memoryPointerIndex.count()};
        memoryPointerIndex.append((stream) -> {
            return pointers.consume((int sortIndex, byte[] key, long timestamp, boolean tombstoned, long version, long pointer) -> {
                count[0]++;
                if (count[0] > maxUpdatesBetweenCompactionHintMarker) { //  TODO flush on memory pressure.
                    count[0] = memoryPointerIndex.count();
                    if (count[0] > maxUpdatesBetweenCompactionHintMarker) { //  TODO flush on memory pressure.
                        commit();
                    }
                }
                return stream.stream(sortIndex, key, timestamp, tombstoned, version, pointer);
            });
        });
        merge();
    }

    private volatile boolean merging = true; // TODO set back to false!

    public void merge() throws Exception {
        int maxMergeDebt = 2; // TODO expose config
        if (mergeablePointerIndexs.mergeDebt() < maxMergeDebt || merging) {
            return;
        }
        long nextIndexId;
        synchronized (this) {
            // TODO use semaphores instead of a hard lock
            if (merging) {
                return;
            }
            merging = true;
            nextIndexId = largestIndexId.incrementAndGet();
        }
        File[] tmpRoot = new File[1];
        mergeablePointerIndexs.merge(maxMergeDebt,
            () -> {
                tmpRoot[0] = Files.createTempDir();
                DiskBackedLeapPointerIndex pointerIndex = new DiskBackedLeapPointerIndex(
                    new DiskBackedPointerIndexFiler(new File(tmpRoot[0], "index").getAbsolutePath(), "rw", false), 64, 4096); //, new DiskBackedPointerIndexFiler(new File(tmpRoot[0], "keys").getAbsolutePath(), "rw", false));
                return pointerIndex;
            },
            (index) -> {
                index.commit();
                index.close();
                File mergedIndexRoot = new File(indexRoot, Long.toString(nextIndexId));
                FileUtils.moveDirectory(tmpRoot[0], mergedIndexRoot);
                File indexFile = new File(mergedIndexRoot, "index");
                File keyFile = new File(mergedIndexRoot, "keys");
                DiskBackedLeapPointerIndex reopenedIndex = new DiskBackedLeapPointerIndex(
                    new DiskBackedPointerIndexFiler(indexFile.getAbsolutePath(), "rw", false), 64, 4096);//,
                //new DiskBackedPointerIndexFiler(keyFile.getAbsolutePath(), "rw", false));
                System.out.println("Commited " + reopenedIndex.count() + " index:" + indexFile.length() + "bytes keys:" + keyFile.length() + "bytes");
                return reopenedIndex;
            });
        merging = false;
    }

    private volatile boolean commiting = false;

    @Override
    public void commit() throws Exception {
        if (memoryPointerIndex.isEmpty() || commiting) {
            return;
        }
        long nextIndexId;
        synchronized (this) { // TODO use semaphores instead of a hard lock
            if (commiting) {
                return;
            }
            commiting = true;
            if (flushingMemoryPointerIndex != null) {
                throw new RuntimeException("Concurrently trying to flush while a flush is underway.");
            }
            nextIndexId = largestIndexId.incrementAndGet();
            flushingMemoryPointerIndex = memoryPointerIndex;
            memoryPointerIndex = new MemoryPointerIndex();
        }
        LOG.info("Commiting memory index (" + flushingMemoryPointerIndex.count() + ") to on disk index." + indexRoot);
        File tmpRoot = Files.createTempDir();
        DiskBackedPointerIndex pointerIndex = new DiskBackedPointerIndex(new DiskBackedPointerIndexFiler(new File(tmpRoot, "index").getAbsolutePath(), "rw",
            false), new DiskBackedPointerIndexFiler(new File(tmpRoot, "keys").getAbsolutePath(), "rw", false));
        pointerIndex.append(flushingMemoryPointerIndex);
        pointerIndex.close();
        File index = new File(indexRoot, Long.toString(nextIndexId));
        FileUtils.moveDirectory(tmpRoot, index);
        pointerIndex = new DiskBackedPointerIndex(new DiskBackedPointerIndexFiler(new File(index, "index").getAbsolutePath(), "rw", false),
            new DiskBackedPointerIndexFiler(new File(index, "keys").getAbsolutePath(), "rw", false));
        synchronized (this) {
            mergeablePointerIndexs.append(pointerIndex);
            flushingMemoryPointerIndex = null;
            commiting = false;
        }
    }

    @Override
    public String toString() {
        return "LSMPointerIndex{"
            + "indexRoot=" + indexRoot
            + ", memoryPointerIndex=" + memoryPointerIndex
            + ", flushingMemoryPointerIndex=" + flushingMemoryPointerIndex
            + ", largestIndexId=" + largestIndexId
            + ", mergeablePointerIndexs=" + mergeablePointerIndexs
            + '}';
    }

}
