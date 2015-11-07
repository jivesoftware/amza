package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.PointerIndexFactory;
import com.jivesoftware.os.amza.lsm.api.RawConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import com.jivesoftware.os.amza.lsm.api.RawPointGet;
import com.jivesoftware.os.amza.lsm.api.RawReadPointerIndex;

/**
 *
 * @author jonathan.colt
 */
public class MergeablePointerIndexs implements RawReadPointerIndex {

    // newest to oldest
    private final Object indexesLock = new Object();
    private RawConcurrentReadablePointerIndex[] indexes = new RawConcurrentReadablePointerIndex[0];
    private long version;

    public static interface CommitIndex {

        DiskBackedLeapPointerIndex commit(DiskBackedLeapPointerIndex index) throws Exception;
    }

    public boolean merge(int count, PointerIndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        RawConcurrentReadablePointerIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        if (copy.length <= count) {
            return false;
        }

        DiskBackedLeapPointerIndex mergedIndex = indexFactory.createPointerIndex();
        RawNextPointer[] feeders = new RawNextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(1024 * 1204 * 10).rowScan();
        }
        InterleavePointerStream feedInterleaver = new InterleavePointerStream(feeders);
        mergedIndex.append((stream) -> {
            while (feedInterleaver.next(stream));
            return true;
        });

        int newSinceMerge;
        synchronized (indexesLock) {
            newSinceMerge = indexes.length - copy.length;
            DiskBackedLeapPointerIndex[] merged = new DiskBackedLeapPointerIndex[newSinceMerge + 1];
            System.arraycopy(indexes, 0, merged, 1, newSinceMerge);
            merged[0] = commitIndex.commit(mergedIndex);
            indexes = merged;
            version++;
        }

        System.out.println("Merged (" + copy.length + "), NewSinceMerge (" + newSinceMerge + ")");

        for (RawConcurrentReadablePointerIndex c : copy) {
            c.destroy();
        }
        return true;
    }

    public int mergeDebt() {
        return grab().length;
    }

    public RawConcurrentReadablePointerIndex[] grab() {
        RawConcurrentReadablePointerIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy;
    }

    public void append(RawConcurrentReadablePointerIndex pointerIndex) {
        synchronized (indexesLock) {
            RawConcurrentReadablePointerIndex[] append = new RawConcurrentReadablePointerIndex[indexes.length + 1];
            append[0] = pointerIndex;
            System.arraycopy(indexes, 0, append, 1, indexes.length);
            indexes = append;
            version++;
        }
    }

    long pointGetCacheVersion;  // HACK
    RawPointGet pointGetCache; // HACK

    @Override
    public RawPointGet getPointer() throws Exception {
        long stackVersion = version;
        if (pointGetCache == null || pointGetCacheVersion < stackVersion) {
            System.out.println("RawPointGet for version " + stackVersion);
            pointGetCache = PointerIndexUtil.get(indexes);
            pointGetCacheVersion = stackVersion;
        }
        return pointGetCache;
    }

    @Override
    public RawNextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        return PointerIndexUtil.rangeScan(indexes, from, to);
    }

    @Override
    public RawNextPointer rowScan() throws Exception {
        return PointerIndexUtil.rowScan(indexes);
    }

    @Override
    public void close() throws Exception {
        synchronized (indexesLock) {
            for (RawConcurrentReadablePointerIndex indexe : indexes) {
                indexe.close();
            }
        }
    }

    @Override
    public long count() throws Exception {
        long count = 0;
        for (RawConcurrentReadablePointerIndex g : grab()) {
            count += g.count();
        }
        return count;
    }

    @Override
    public boolean isEmpty() throws Exception {
        for (RawConcurrentReadablePointerIndex g : grab()) {
            if (!g.isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
