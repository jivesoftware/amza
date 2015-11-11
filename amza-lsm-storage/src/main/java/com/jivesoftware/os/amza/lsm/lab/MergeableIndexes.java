package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.lsm.lab.api.CommitIndex;
import com.jivesoftware.os.amza.lsm.lab.api.IndexFactory;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.Callable;

/**
 *
 * @author jonathan.colt
 */
public class MergeableIndexes {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // newest to oldest
    private final Object indexesLock = new Object();
    private volatile boolean[] merging = new boolean[0];
    private volatile RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[0];
    private volatile long version;

    public void append(RawConcurrentReadableIndex pointerIndex) {
        synchronized (indexesLock) {
            boolean[] prependToMerging = new boolean[indexes.length + 1];
            prependToMerging[0] = false;
            System.arraycopy(merging, 0, prependToMerging, 1, merging.length);

            RawConcurrentReadableIndex[] prependToIndexes = new RawConcurrentReadableIndex[indexes.length + 1];
            prependToIndexes[0] = pointerIndex;
            System.arraycopy(indexes, 0, prependToIndexes, 1, indexes.length);

            merging = prependToMerging;
            indexes = prependToIndexes;
            version++;
        }
    }

    public int mergeDebt() {
        Object[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy.length;
    }

    public static void main(String[] args) {
        int[] sizes = new int[]{1000000, 1000000, 1000000, 1000000, 2000001};

        for (int i = 0; i < sizes.length; i++) {
            long c = 0;
            int s = 0;
            for (int j = i; j < sizes.length; j++) {
                c += sizes[j];
                s++;
                if (s > 1) {
                    System.out.println(i + "-" + j + "=" + (((c / (double) s)) - s));
                }
            }
        }
    }

    public Merger merge(IndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        boolean[] merginCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        synchronized (indexesLock) {
            merginCopy = merging;
            indexesCopy = indexes;
        }
        if (indexesCopy.length <= 1) {
            return null;
        }

        int startOfSmallestMerge = -1;
        int length = 0;

        boolean smallestPairs = false;
        boolean biggestGain = true;

        if (smallestPairs) {
            long smallestCount = Long.MAX_VALUE;
            for (int i = 0; i < indexesCopy.length - 1; i++) {
                if (merginCopy[i]) {
                    continue;
                }
                long worstCaseMergeCount = indexesCopy[i].count() + indexesCopy[i + 1].count();
                if (worstCaseMergeCount < smallestCount) {
                    smallestCount = worstCaseMergeCount;
                    startOfSmallestMerge = i;
                    length = 2;
                }
            }
        } else if (biggestGain) {
            if (indexesCopy.length == 2) {
                startOfSmallestMerge = 0;
                length = 2;
            } else {
                double smallestScore = Double.MAX_VALUE;
                NEXT:
                for (int i = 0; i < indexesCopy.length; i++) {
                    long worstCaseMergeCount = 0;
                    int fileCount = 0;
                    for (int j = i; j < indexesCopy.length; j++) {
                        if (merginCopy[j]) {
                            continue NEXT;
                        }
                        worstCaseMergeCount += indexesCopy[j].count();
                        fileCount++;

                        double score = (((worstCaseMergeCount / (double) fileCount)) - fileCount);
                        if (score < smallestScore && fileCount > 1) {
                            smallestScore = score;
                            startOfSmallestMerge = i;
                            length = fileCount;
                        }
                    }
                }
            }
        }

        if (startOfSmallestMerge == -1 || length < 2) {
            return null;
        }

        RawConcurrentReadableIndex[] mergeSet = new RawConcurrentReadableIndex[length];
        for (int i = 0; i < length; i++) {
            mergeSet[i] = indexesCopy[startOfSmallestMerge + i];

        }

        // prevent others from trying to merge the same things
        synchronized (indexesLock) {
            int mi = 0;
            for (int i = 0; i < indexes.length && mi < mergeSet.length; i++) {
                if (indexes[i] == mergeSet[mi]) {
                    merging[i] = true;
                    mi++;
                }
            }
        }

        IndexRangeId join = null;
        for (RawConcurrentReadableIndex m : mergeSet) {
            if (join == null) {
                join = m.id();
            } else {
                join = join.join(m.id());
            }
        }

        return new Merger(mergeSet, join, indexFactory, commitIndex);
    }

    public class Merger implements Callable<Void> {

        private final RawConcurrentReadableIndex[] mergeSet;
        private final IndexRangeId mergeRangeId;
        private final IndexFactory indexFactory;
        private final CommitIndex commitIndex;

        private Merger(RawConcurrentReadableIndex[] merging,
            IndexRangeId mergeRangeId,
            IndexFactory indexFactory,
            CommitIndex commitIndex) {
            this.mergeSet = merging;
            this.mergeRangeId = mergeRangeId;
            this.indexFactory = indexFactory;
            this.commitIndex = commitIndex;
        }

        @Override
        public Void call() {
            try {
                long startMerge = System.currentTimeMillis();

                long worstCaseCount = 0;
                NextRawEntry[] feeders = new NextRawEntry[mergeSet.length];
                IndexRangeId superRange = null;
                for (int i = 0; i < feeders.length; i++) {
                    IndexRangeId id = mergeSet[i].id();
                    if (superRange == null) {
                        superRange = id;
                    } else {
                        superRange = superRange.join(id);
                    }
                    ReadIndex readIndex = mergeSet[i].reader(1024 * 1024); // TODO config
                    worstCaseCount += readIndex.count();
                    feeders[i] = readIndex.rowScan();
                }

                WriteLeapsAndBoundsIndex mergedIndex = indexFactory.createIndex(superRange, worstCaseCount);
                InterleaveStream feedInterleaver = new InterleaveStream(feeders);
                mergedIndex.append((stream) -> {
                    while (feedInterleaver.next(stream));
                    return true;
                });
                mergedIndex.close();

                LeapsAndBoundsIndex index = commitIndex.commit(superRange, mergedIndex);

                synchronized (indexesLock) {
                    int newLength = indexes.length - (mergeSet.length - 1);
                    boolean[] updateMerging = new boolean[newLength];
                    RawConcurrentReadableIndex[] updateIndexs = new RawConcurrentReadableIndex[newLength];

                    int ui = 0;
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            if (mi == 0) {
                                updateMerging[ui] = false;
                                updateIndexs[ui] = index;
                                ui++;
                            }
                            mi++;
                        } else {
                            updateMerging[ui] = merging[i];
                            updateIndexs[ui] = indexes[i];
                            ui++;
                        }

                    }

                    merging = updateMerging;
                    indexes = updateIndexs;
                    version++;
                }

                StringBuilder sb = new StringBuilder();
                sb.append((System.currentTimeMillis() - startMerge) + " millis Merged:");
                for (RawConcurrentReadableIndex m : mergeSet) {
                    sb.append(m.id()).append("=").append(m.count()).append(",");
                    m.destroy();
                }
                sb.append(" remaing debt:" + mergeDebt() + " -> ");
                for (RawConcurrentReadableIndex i : indexes) {
                    sb.append(i.id()).append("=").append(i.count()).append(",");
                }
                System.out.println("\n" + sb.toString() + "\n");
            } catch (Exception x) {
                LOG.warn("Failed to merge range:" + mergeRangeId, x);

                synchronized (indexesLock) {
                    int mi = 0;
                    for (int i = 0; i < indexes.length && mi < mergeSet.length; i++) {
                        if (indexes[i] == mergeSet[mi]) {
                            merging[i] = false;
                            mi++;
                        }
                    }
                }

            }
            return null;
        }

    }

    public final Reader reader() throws Exception {
        return new Reader();
    }

    public class Reader {

        private long cacheVersion = -1;
        private RawConcurrentReadableIndex[] stackIndexes;
        private ReadIndex[] readIndexs;

        public ReadIndex[] acquire(int bufferSize) throws Exception {
            long stackVersion = version;
            TRY_AGAIN:
            while (true) {
                if (cacheVersion < stackVersion) {
                    readIndexs = acquireReadIndexes(bufferSize);
                }
                for (int i = 0; i < readIndexs.length; i++) {
                    ReadIndex readIndex = readIndexs[i];
                    try {
                        readIndex.acquire();
                    } catch (Throwable t) {
                        for (int j = 0; j < i; j++) {
                            readIndexs[j].release();
                        }
                        if (t instanceof InterruptedException) {
                            throw t;
                        }
                        continue TRY_AGAIN;
                    }
                }
                return readIndexs;
            }
        }

        private ReadIndex[] acquireReadIndexes(int bufferSize) throws Exception {
            TRY_AGAIN:
            while (true) {
                long stackVersion = version;
                stackIndexes = indexes;
                cacheVersion = stackVersion;
                ReadIndex[] readIndexs = new ReadIndex[stackIndexes.length];
                for (int i = 0; i < readIndexs.length; i++) {
                    readIndexs[i] = stackIndexes[i].reader(bufferSize);
                    if (readIndexs[i] == null) {
                        continue TRY_AGAIN;
                    }
                }
                return readIndexs;
            }
        }

        public void release() {
            for (ReadIndex readIndex : readIndexs) {
                readIndex.release();
            }
        }
    }

    public long count() throws Exception {
        long count = 0;
        for (RawConcurrentReadableIndex g : grab()) {
            count += g.count();
        }
        return count;
    }

    public void close() throws Exception {
        synchronized (indexesLock) {
            for (RawConcurrentReadableIndex indexe : indexes) {
                indexe.close();
            }
        }
    }

    public boolean isEmpty() throws Exception {
        for (RawConcurrentReadableIndex g : grab()) {
            if (!g.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private RawConcurrentReadableIndex[] grab() {
        RawConcurrentReadableIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy;
    }

}
