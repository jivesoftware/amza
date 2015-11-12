package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.lsm.lab.api.CommitIndex;
import com.jivesoftware.os.amza.lsm.lab.api.IndexFactory;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
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
            int length = indexes.length + 1;
            boolean[] prependToMerging = new boolean[length];
            prependToMerging[0] = false;
            System.arraycopy(merging, 0, prependToMerging, 1, merging.length);

            RawConcurrentReadableIndex[] prependToIndexes = new RawConcurrentReadableIndex[length];
            prependToIndexes[0] = pointerIndex;
            System.arraycopy(indexes, 0, prependToIndexes, 1, indexes.length);

            merging = prependToMerging;
            indexes = prependToIndexes;
            version++;
        }
    }

    public boolean hasMergeDebt() throws IOException {
        boolean[] mergingCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        synchronized (indexesLock) {
            mergingCopy = merging;
            indexesCopy = indexes;
        }

        MergeRange mergeRange = getMergeRange(MergeStrategy.crazySauce, mergingCopy, indexesCopy);
        if (mergeRange.startOfSmallestMerge < 0) {
            for (boolean m : mergingCopy) {
                if (m) {
                    return true;
                }
            }
            return false;
        } else {
            return true;
        }
    }

    public int mergeDebt() {
        RawConcurrentReadableIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy.length;
    }

    public static void main(String[] args) {
        int[] sizes = new int[] { 1000000, 1000000, 1000000, 1000000, 2000001 };

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

    enum MergeStrategy {
        smallestPairs,
        biggestGain,
        longestRun,
        crazySauce;
    }

    public Merger merge(IndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        boolean[] mergingCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        synchronized (indexesLock) {
            mergingCopy = merging;
            indexesCopy = indexes;
        }
        if (indexesCopy.length <= 1) {
            return null;
        }

        MergeRange mergeRange = getMergeRange(MergeStrategy.crazySauce, mergingCopy, indexesCopy);
        int startOfSmallestMerge = mergeRange.startOfSmallestMerge;
        int length = mergeRange.length;

        if (startOfSmallestMerge == -1 || length < 2) {
            return null;
        }

        RawConcurrentReadableIndex[] mergeSet = new RawConcurrentReadableIndex[length];
        for (int i = 0; i < length; i++) {
            mergeSet[i] = indexesCopy[startOfSmallestMerge + i];
        }

        // prevent others from trying to merge the same things
        synchronized (indexesLock) {
            boolean[] updateMerging = new boolean[merging.length];
            int mi = 0;
            for (int i = 0; i < indexes.length; i++) {
                if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                    updateMerging[i] = true;
                    mi++;
                } else {
                    updateMerging[i] = merging[i];
                }
            }

            /*System.out.println("????? WTF " + startOfSmallestMerge + " " + length + " " + Arrays.toString(mergingCopy) + " ~~~~~~~~~~~ " + Arrays.toString(
                updateMerging));*/

            merging = updateMerging;
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
                /*StringBuilder bla = new StringBuilder();
                bla.append(Thread.currentThread() + " ------ Merging:");
                for (RawConcurrentReadableIndex m : mergeSet) {
                    bla.append(m.id()).append("=").append(m.count()).append(",");
                }
                bla.append(" remaining debt:" + mergeDebt() + " -> ");
                for (RawConcurrentReadableIndex i : indexes) {
                    bla.append(i.id()).append("=").append(i.count()).append(",");
                }
                System.out.println("\n" + bla.toString() + "\n");*/

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
                    while (feedInterleaver.next(stream)) ;
                    return true;
                });
                mergedIndex.close();

                LeapsAndBoundsIndex index = commitIndex.commit(superRange, mergedIndex);

                synchronized (indexesLock) {
                    int newLength = indexes.length - (mergeSet.length - 1);
                    boolean[] updateMerging = new boolean[newLength];
                    RawConcurrentReadableIndex[] updateIndexes = new RawConcurrentReadableIndex[newLength];

                    int ui = 0;
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            if (mi == 0) {
                                updateMerging[ui] = false;
                                updateIndexes[ui] = index;
                                ui++;
                            }
                            mi++;
                        } else {
                            updateMerging[ui] = merging[i];
                            updateIndexes[ui] = indexes[i];
                            ui++;
                        }
                    }

                    merging = updateMerging;
                    indexes = updateIndexes;
                    version++;
                }

                StringBuilder sb = new StringBuilder();
                sb.append((System.currentTimeMillis() - startMerge) + " millis Merged:");
                for (RawConcurrentReadableIndex m : mergeSet) {
                    sb.append(m.id()).append("=").append(m.count()).append(",");
                    m.destroy();
                }
                sb.append(" remaining debt:" + mergeDebt() + " -> ");
                for (int i = 0; i < indexes.length; i++) {
                    RawConcurrentReadableIndex rawIndex = indexes[i];
                    sb.append(rawIndex.id()).append("=").append(rawIndex.count()).append(merging[i] ? "*" : "").append(",");
                }
                System.out.println("\n" + sb.toString() + "\n");
            } catch (Exception x) {
                LOG.warn("Failed to merge range:" + mergeRangeId, x);

                synchronized (indexesLock) {
                    boolean[] updateMerging = new boolean[merging.length];
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            updateMerging[i] = false;
                            mi++;
                        } else {
                            updateMerging[i] = merging[i];
                        }
                    }
                    merging = updateMerging;
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

    private static class MergeRange {
        private final int startOfSmallestMerge;
        private final int length;

        public MergeRange(int startOfSmallestMerge, int length) {
            this.startOfSmallestMerge = startOfSmallestMerge;
            this.length = length;
        }
    }

    public MergeRange getMergeRange(MergeStrategy mergeStrategy, boolean[] mergingCopy,
        RawConcurrentReadableIndex[] indexesCopy) throws IOException {

        int startOfSmallestMerge = -1;
        int length = 0;

        if (mergeStrategy == MergeStrategy.smallestPairs) {
            long smallestCount = Long.MAX_VALUE;
            for (int i = 0; i < indexesCopy.length - 1; i++) {
                if (mergingCopy[i] || mergingCopy[i + 1]) {
                    continue;
                }
                long worstCaseMergeCount = indexesCopy[i].count() + indexesCopy[i + 1].count();
                if (worstCaseMergeCount < smallestCount) {
                    smallestCount = worstCaseMergeCount;
                    startOfSmallestMerge = i;
                    length = 2;
                }
            }
        } else if (mergeStrategy == MergeStrategy.biggestGain) {
            if (indexesCopy.length == 2 && !mergingCopy[0] && !mergingCopy[1]) {
                startOfSmallestMerge = 0;
                length = 2;
            } else {
                double smallestScore = Double.MAX_VALUE;
                NEXT:
                for (int i = 0; i < indexesCopy.length; i++) {
                    long worstCaseMergeCount = 0;
                    int fileCount = 0;
                    for (int j = i; j < indexesCopy.length; j++) {
                        if (mergingCopy[j]) {
                            continue NEXT;
                        }
                        worstCaseMergeCount += indexesCopy[j].count();
                        fileCount++;

                        if (fileCount > 1) {
                            double score = (((worstCaseMergeCount / (double) fileCount)) - fileCount);
                            if (score < smallestScore) {
                                smallestScore = score;
                                startOfSmallestMerge = i;
                                length = fileCount;
                            }
                        }
                    }
                }
            }
        } else if (mergeStrategy == MergeStrategy.longestRun) {
            int longestRun = 0;
            if (!mergingCopy[0]) {
                for (int i = 1; i < indexesCopy.length; i++) {
                    if (mergingCopy[i]) {
                        break;
                    }
                    int run = i + 1;
                    if (run > longestRun) {
                        longestRun = run;
                        startOfSmallestMerge = 0;
                        length = run;
                    }
                }
            }
            if (!mergingCopy[mergingCopy.length - 1]) {
                for (int i = indexesCopy.length - 2; i >= 0; i--) {
                    if (mergingCopy[i]) {
                        break;
                    }
                    int run = indexesCopy.length - i;
                    if (run > longestRun) {
                        longestRun = run;
                        startOfSmallestMerge = i;
                        length = run;
                    }
                }
            }
        } else if (mergeStrategy == MergeStrategy.crazySauce) {
            boolean encounteredMerge = false;
            for (int i = 1; i < indexesCopy.length; i++) {
                if (mergingCopy[i]) {
                    encounteredMerge = true;
                }

                long headSum = 0;
                for (int j = 0; j < i; j++) {
                    headSum += indexesCopy[j].count();
                }

                if (headSum >= indexesCopy[i].count()) {
                    if (encounteredMerge) {
                        startOfSmallestMerge = -1;
                        length = 0;
                        break;
                    } else {
                        startOfSmallestMerge = 0;
                        length = 1 + i;
                    }
                }
            }
            if (startOfSmallestMerge < 0 && indexesCopy.length > 1 && !encounteredMerge) {
                startOfSmallestMerge = 0;
                length = indexesCopy.length;
            }
        }
        return new MergeRange(startOfSmallestMerge, length);
    }
}
