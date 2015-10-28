package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerIndexFactory;
import com.jivesoftware.os.amza.lsm.api.ReadPointerIndex;

/**
 *
 * @author jonathan.colt
 */
public class MergeablePointerIndexs implements ReadPointerIndex {

    // newest to oldest
    private final Object indexesLock = new Object();
    private ConcurrentReadablePointerIndex[] indexes = new ConcurrentReadablePointerIndex[0];

    public static interface CommitIndex {

        DiskBackedPointerIndex commit(DiskBackedPointerIndex index) throws Exception;
    }

    public boolean merge(int count, PointerIndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        ConcurrentReadablePointerIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        if (copy.length <= count) {
            return false;
        }

        DiskBackedPointerIndex mergedIndex = indexFactory.createPointerIndex();
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rowScan();
        }
        InterleavePointerStream feedInterleaver = new InterleavePointerStream(feeders);
        mergedIndex.append((stream) -> {
            while (feedInterleaver.next(stream));
            return true;
        });

        int newSinceMerge;
        synchronized (indexesLock) {
            newSinceMerge = indexes.length - copy.length;
            DiskBackedPointerIndex[] merged = new DiskBackedPointerIndex[newSinceMerge + 1];
            System.arraycopy(indexes, 0, merged, 1, newSinceMerge);
            merged[0] = commitIndex.commit(mergedIndex);
            indexes = merged;
        }

        System.out.println("Merged (" + copy.length + "), NewSinceMerge (" + newSinceMerge + ")");

        for (ConcurrentReadablePointerIndex c : copy) {
            c.destroy();
        }
        return true;
    }

    public int mergeDebut() {
        return grab().length;
    }

    public ConcurrentReadablePointerIndex[] grab() {
        ConcurrentReadablePointerIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy;
    }

    public void append(ConcurrentReadablePointerIndex pointerIndex) {
        synchronized (indexesLock) {
            ConcurrentReadablePointerIndex[] append = new ConcurrentReadablePointerIndex[indexes.length + 1];
            append[0] = pointerIndex;
            System.arraycopy(indexes, 0, append, 1, indexes.length);
            indexes = append;
        }
    }

    @Override
    public NextPointer getPointer(byte[] key) throws Exception {
        return PointerIndexUtil.get(indexes, key);
    }

    @Override
    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        return PointerIndexUtil.rangeScan(indexes, from, to);
    }

    @Override
    public NextPointer rowScan() throws Exception {
        return PointerIndexUtil.rowScan(indexes);
    }

    @Override
    public void close() throws Exception {
        synchronized (indexesLock) {
            for (ConcurrentReadablePointerIndex indexe : indexes) {
                indexe.close();
            }
        }
    }

    @Override
    public long count() throws Exception {
        long count = 0;
        for (ConcurrentReadablePointerIndex g : grab()) {
            count += g.count();
        }
        return count;
    }

    @Override
    public boolean isEmpty() throws Exception {
        for (ConcurrentReadablePointerIndex g : grab()) {
            if (!g.isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
