package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.lsm.lab.api.CommitIndex;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.IndexFactory;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public class MergeableIndexes implements ReadIndex {

    // newest to oldest
    private final Object indexesLock = new Object();
    private RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[0];
    private long version;

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
        for (int i = 0; i < feeders.length; i++) {
            ReadIndex readIndex = copy[i].reader(1024 * 1204 * 10);
            worstCaseCount += readIndex.count();
            feeders[i] = readIndex.rowScan();
        }

        LeapsAndBoundsIndex mergedIndex = indexFactory.createIndex(worstCaseCount);
        InterleaveStream feedInterleaver = new InterleaveStream(feeders);
        mergedIndex.append((stream) -> {
            while (feedInterleaver.next(stream));
            return true;
        });

        int newSinceMerge;
        synchronized (indexesLock) {
            newSinceMerge = indexes.length - copy.length;
            LeapsAndBoundsIndex[] merged = new LeapsAndBoundsIndex[newSinceMerge + 1];
            System.arraycopy(indexes, 0, merged, 1, newSinceMerge);
            merged[0] = commitIndex.commit(mergedIndex);
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
        return grab().length;
    }

    public RawConcurrentReadableIndex[] grab() {
        RawConcurrentReadableIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy;
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

    private long pointGetCacheVersion;  // HACK
    private GetRaw pointGetCache; // HACK

    @Override
    public GetRaw get() throws Exception {
        long stackVersion = version;
        if (pointGetCache == null || pointGetCacheVersion < stackVersion) {
            System.out.println("RawPointGet for version " + stackVersion);
            pointGetCache = IndexUtil.get(indexes);
            pointGetCacheVersion = stackVersion;
        }
        return pointGetCache;
    }

    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        return IndexUtil.rangeScan(indexes, from, to);
    }

    @Override
    public NextRawEntry rowScan() throws Exception {
        return IndexUtil.rowScan(indexes);
    }

    @Override
    public void close() throws Exception {
        synchronized (indexesLock) {
            for (RawConcurrentReadableIndex indexe : indexes) {
                indexe.close();
            }
        }
    }

    @Override
    public long count() throws Exception {
        long count = 0;
        for (RawConcurrentReadableIndex g : grab()) {
            count += g.count();
        }
        return count;
    }

    @Override
    public boolean isEmpty() throws Exception {
        for (RawConcurrentReadableIndex g : grab()) {
            if (!g.isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
