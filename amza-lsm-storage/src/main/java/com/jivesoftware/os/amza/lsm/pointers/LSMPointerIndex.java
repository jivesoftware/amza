package com.jivesoftware.os.amza.lsm.pointers;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.lsm.lab.IndexFile;
import com.jivesoftware.os.amza.lsm.lab.IndexUtil;
import com.jivesoftware.os.amza.lsm.lab.LeapsAndBoundsIndex;
import com.jivesoftware.os.amza.lsm.lab.MergeableIndexes;
import com.jivesoftware.os.amza.lsm.lab.RawMemoryIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.pointers.api.NextPointer;
import com.jivesoftware.os.amza.lsm.pointers.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.pointers.api.Pointers;
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
    private RawMemoryIndex memoryPointerIndex;
    private RawMemoryIndex flushingMemoryPointerIndex;
    private final AtomicLong largestIndexId = new AtomicLong();
    private final MergeableIndexes mergeablePointerIndexs;

    private final LSMValueMarshaller marshaller = new LSMValueMarshaller();

    public LSMPointerIndex(File indexRoot, int maxUpdatesBetweenCompactionHintMarker) throws IOException {
        this.indexRoot = indexRoot;
        this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
        this.memoryPointerIndex = new RawMemoryIndex(marshaller);
        this.mergeablePointerIndexs = new MergeableIndexes();
        TreeSet<Long> indexIds = new TreeSet<>();
        for (File indexFile : indexRoot.listFiles()) {
            long indexId = Long.parseLong(FilenameUtils.removeExtension(indexFile.getName()));
            indexIds.add(indexId);
            if (largestIndexId.get() < indexId) {
                largestIndexId.set(indexId);
            }
        }
        for (Long indexId : indexIds) {
            File index = new File(indexRoot, String.valueOf(indexId));
            LeapsAndBoundsIndex pointerIndex = new LeapsAndBoundsIndex(
                new IndexFile(new File(index, "index").getAbsolutePath(), "rw", false), 64, 1024);
            mergeablePointerIndexs.append(pointerIndex);
        }
    } // descending

    private RawConcurrentReadableIndex[] grab() {
        RawMemoryIndex stackCopy = flushingMemoryPointerIndex;
        int flushing = stackCopy == null ? 0 : 1;
        RawConcurrentReadableIndex[] grabbed = mergeablePointerIndexs.grab();
        RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[grabbed.length + 1 + flushing];
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
        return LSMPointerUtils.rawToReal(key, IndexUtil.get(grab()));
    }

    @Override
    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        return LSMPointerUtils.rawToReal(IndexUtil.rangeScan(grab(), from, to));
    }

    @Override
    public NextPointer rowScan() throws Exception {
        return LSMPointerUtils.rawToReal(IndexUtil.rowScan(grab()));
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
    public boolean append(Pointers pointers) throws Exception {
        long[] count = {memoryPointerIndex.count()};

        boolean appended = memoryPointerIndex.append((stream) -> {
            return pointers.consume((key, timestamp, tombstoned, version, pointer) -> {
                count[0]++;
                if (count[0] > maxUpdatesBetweenCompactionHintMarker) { //  TODO flush on memory pressure.
                    count[0] = memoryPointerIndex.count();
                    if (count[0] > maxUpdatesBetweenCompactionHintMarker) { //  TODO flush on memory pressure.
                        commit();
                    }
                }
                byte[] rawEntry = marshaller.toRawEntry(key, timestamp, tombstoned, version, pointer);
                return stream.stream(rawEntry, 0, rawEntry.length);
            });
        });
        merge();
        return appended;
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
            (count) -> {
                tmpRoot[0] = Files.createTempDir();

                int entriesBetweenLeaps = 4096; // TODO expose to a config;
                
                int maxLeaps = IndexUtil.calculateIdealMaxLeaps(count, entriesBetweenLeaps);

                LeapsAndBoundsIndex pointerIndex = new LeapsAndBoundsIndex(
                    new IndexFile(new File(tmpRoot[0], "index").getAbsolutePath(), "rw", false), maxLeaps, 4096);
                return pointerIndex;
            },
            (index) -> {
                index.commit();
                index.close();
                File mergedIndexRoot = new File(indexRoot, Long.toString(nextIndexId));
                FileUtils.moveDirectory(tmpRoot[0], mergedIndexRoot);
                File indexFile = new File(mergedIndexRoot, "index");
                File keyFile = new File(mergedIndexRoot, "keys");
                LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(
                    new IndexFile(indexFile.getAbsolutePath(), "rw", false), 64, 4096);//,
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
            memoryPointerIndex = new RawMemoryIndex(marshaller);
        }
        LOG.info("Commiting memory index (" + flushingMemoryPointerIndex.count() + ") to on disk index." + indexRoot);
        File tmpRoot = Files.createTempDir();
        LeapsAndBoundsIndex pointerIndex = new LeapsAndBoundsIndex(
            new IndexFile(new File(tmpRoot, "index").getAbsolutePath(), "rw", false), 64, 100); // TODO Config
        pointerIndex.append(flushingMemoryPointerIndex);
        pointerIndex.close();
        File index = new File(indexRoot, Long.toString(nextIndexId));
        FileUtils.moveDirectory(tmpRoot, index);
        pointerIndex = new LeapsAndBoundsIndex(new IndexFile(new File(index, "index").getAbsolutePath(), "rw", false), 64, 100); // TODO Config
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
