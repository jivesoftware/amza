package com.jivesoftware.os.amza.lsm.pointers;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.lsm.lab.IndexFile;
import com.jivesoftware.os.amza.lsm.lab.IndexRangeId;
import com.jivesoftware.os.amza.lsm.lab.IndexUtil;
import com.jivesoftware.os.amza.lsm.lab.LeapsAndBoundsIndex;
import com.jivesoftware.os.amza.lsm.lab.MergeableIndexes;
import com.jivesoftware.os.amza.lsm.lab.RawMemoryIndex;
import com.jivesoftware.os.amza.lsm.lab.WriteLeapsAndBoundsIndex;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.amza.lsm.pointers.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.pointers.api.Pointers;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class LSMPointerIndex implements PointerIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService destroy;
    private final ExecutorService merge;
    private final File indexRoot;
    private final int maxUpdatesBetweenCompactionHintMarker;
    private RawMemoryIndex memoryPointerIndex;
    private RawMemoryIndex flushingMemoryPointerIndex;
    private final AtomicLong largestIndexId = new AtomicLong();
    private final MergeableIndexes mergeablePointerIndexs;

    private final LSMValueMarshaller marshaller = new LSMValueMarshaller();

    public LSMPointerIndex(ExecutorService merge, ExecutorService destroy, File indexRoot, int maxUpdatesBetweenCompactionHintMarker) throws Exception {
        this.merge = merge;
        this.destroy = destroy;
        this.indexRoot = indexRoot;
        this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
        this.memoryPointerIndex = new RawMemoryIndex(marshaller);
        this.mergeablePointerIndexs = new MergeableIndexes();
        TreeSet<IndexRangeId> ranges = new TreeSet<>();
        for (File indexDir : indexRoot.listFiles()) {
            String rawRange = indexDir.getName();
            String[] range = rawRange.split("-");
            long start = Long.parseLong(range[0]);
            long end = Long.parseLong(range[1]);
            ranges.add(new IndexRangeId(start, end));
            if (largestIndexId.get() < end) {
                largestIndexId.set(end);
            }
        }

        IndexRangeId active = null;
        TreeSet<IndexRangeId> remove = new TreeSet<>();
        for (IndexRangeId range : ranges) {
            if (active == null || !active.intersects(range)) {
                active = range;
            } else {
                LOG.info("Destroying index for overlaping range:" + range);
                remove.add(range);
            }
        }

        for (IndexRangeId range : remove) {
            File indexDir = range.toFile(indexRoot);
            FileUtils.deleteDirectory(indexDir);
        }

        ranges.removeAll(remove);

        for (IndexRangeId range : ranges) {
            File indexDir = range.toFile(indexRoot);
            LeapsAndBoundsIndex pointerIndex = new LeapsAndBoundsIndex(destroy,
                range, new IndexFile(new File(indexDir, "index").getAbsolutePath(), "rw", false));
            mergeablePointerIndexs.append(pointerIndex);
        }
    } // descending

    interface GrabTx<R> {

        R grabbed(ReadIndex[] readIndexes) throws Exception;
    }

    private <R> R grab(int bufferSize, GrabTx<R> grabTx) throws Exception {
        RawMemoryIndex stackCopy = flushingMemoryPointerIndex;
        int flushing = stackCopy == null ? 0 : 1;

        ReadIndex memoryReadIndex = memoryPointerIndex.reader(bufferSize);
        MergeableIndexes.Reader reader = mergeablePointerIndexs.reader();
        memoryReadIndex.acquire();
        try {
            ReadIndex[] acquired = reader.acquire(bufferSize);
            try {
                ReadIndex[] indexes = new ReadIndex[acquired.length + 1 + flushing];
                synchronized (this) {
                    indexes[0] = memoryPointerIndex.reader(bufferSize);
                    if (stackCopy != null) {
                        indexes[1] = stackCopy.reader(bufferSize);
                    }
                }
                System.arraycopy(acquired, 0, indexes, 1 + flushing, acquired.length);
                return grabTx.grabbed(indexes);
            } finally {
                reader.release();
            }
        } finally {
            memoryReadIndex.release();
        }
    }

    @Override
    public <R> R getPointer(byte[] key, PointerTx<R> tx) throws Exception {
        return grab(2048, (readIndexes) -> {
            GetRaw getRaw = IndexUtil.get(readIndexes);
            return LSMPointerUtils.rawToReal(key, getRaw, tx);
        });
    }

    @Override
    public <R> R rangeScan(byte[] from, byte[] to, PointerTx<R> tx) throws Exception {
        return grab(2048, (readIndexes) -> {
            return LSMPointerUtils.rawToReal(IndexUtil.rangeScan(readIndexes, from, to), tx);
        });
    }

    @Override
    public <R> R rowScan(PointerTx<R> tx) throws Exception {
        return grab(2048, (readIndexes) -> {
            return LSMPointerUtils.rawToReal(IndexUtil.rowScan(readIndexes), tx);
        });
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

    private volatile boolean merging = false;

    public void merge() throws Exception {
        int maxMergeDebt = 2; // TODO expose config
        if (mergeablePointerIndexs.mergeDebt() < maxMergeDebt || merging) {
            return;
        }
        synchronized (this) {
            // TODO use semaphores instead of a hard lock
            if (merging) {
                return;
            }
            merging = true;
        }
        File[] tmpRoot = new File[1];
        MergeableIndexes.Merger merger = mergeablePointerIndexs.merge((id, count) -> {
            tmpRoot[0] = Files.createTempDir();

            int entriesBetweenLeaps = 4096; // TODO expose to a config;
            int maxLeaps = IndexUtil.calculateIdealMaxLeaps(count, entriesBetweenLeaps);

            WriteLeapsAndBoundsIndex writeLeapsAndBoundsIndex = new WriteLeapsAndBoundsIndex(id,
                new IndexFile(new File(tmpRoot[0], "index").getAbsolutePath(), "rw", false), maxLeaps, 4096);
            return writeLeapsAndBoundsIndex;
        },
            (id, index) -> {
                File mergedIndexRoot = id.toFile(indexRoot);
                FileUtils.moveDirectory(tmpRoot[0], mergedIndexRoot);
                File indexFile = new File(mergedIndexRoot, "index");
                LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(destroy,
                    id, new IndexFile(indexFile.getAbsolutePath(), "r", false));
                System.out.println("Commited " + reopenedIndex.count() + " index:" + indexFile.length() + "bytes");
                return reopenedIndex;
            });
        if (merger != null) {
            Future<Void> merging = merge.submit(merger);
            merging.get(); // TODO figure out it we really need to wait here.
        }
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

        int entriesBetweenLeaps = 4096; // TODO expose to a config;
        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(flushingMemoryPointerIndex.count(), entriesBetweenLeaps);

        IndexRangeId indexRangeId = new IndexRangeId(nextIndexId, nextIndexId);
        WriteLeapsAndBoundsIndex write = new WriteLeapsAndBoundsIndex(indexRangeId,
            new IndexFile(new File(tmpRoot, "index").getAbsolutePath(), "rw", false), maxLeaps, entriesBetweenLeaps);
        write.append(flushingMemoryPointerIndex);
        write.close();

        File index = indexRangeId.toFile(indexRoot);
        FileUtils.moveDirectory(tmpRoot, index);
        LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(destroy,
            indexRangeId, new IndexFile(new File(index, "index").getAbsolutePath(), "r", false));
        synchronized (this) {
            mergeablePointerIndexs.append(reopenedIndex);
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
