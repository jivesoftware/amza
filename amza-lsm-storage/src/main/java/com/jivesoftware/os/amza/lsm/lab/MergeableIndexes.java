package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.lsm.lab.api.CommitIndex;
import com.jivesoftware.os.amza.lsm.lab.api.IndexFactory;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public class MergeableIndexes {

    // newest to oldest
    private final Object indexesLock = new Object();
    private volatile RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[0];
    private volatile long version;

    public boolean merge(int count, IndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        RawConcurrentReadableIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        if (copy.length <= count) {
            return false;
        }

        long worstCaseCount = 0;
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        IndexRangeId superRange = null;
        for (int i = 0; i < feeders.length; i++) {
            IndexRangeId id = copy[i].id();
            if (superRange == null) {
                superRange = id;
            } else {
                superRange = superRange.join(id);
            }
            ReadIndex readIndex = copy[i].reader(1024 * 1024); // TODO config
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

        int newSinceMerge;
        synchronized (indexesLock) {
            newSinceMerge = indexes.length - copy.length;
            LeapsAndBoundsIndex[] merged = new LeapsAndBoundsIndex[newSinceMerge + 1];
            System.arraycopy(indexes, 0, merged, 1, newSinceMerge);
            merged[0] = commitIndex.commit(superRange, mergedIndex);
            indexes = merged;
            version++;
        }

        System.out.println("Merged (" + copy.length + "), NewSinceMerge (" + newSinceMerge + ")");
        for (RawConcurrentReadableIndex c : copy) {
            c.destroy();
        }
        return true;
    }

    public int mergeDebt() {
        RawConcurrentReadableIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy.length;
    }

    public void append(RawConcurrentReadableIndex pointerIndex) {
        synchronized (indexesLock) {
            RawConcurrentReadableIndex[] append = new RawConcurrentReadableIndex[indexes.length + 1];
            append[0] = pointerIndex;
            System.arraycopy(indexes, 0, append, 1, indexes.length);
            indexes = append;
            version++;
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
