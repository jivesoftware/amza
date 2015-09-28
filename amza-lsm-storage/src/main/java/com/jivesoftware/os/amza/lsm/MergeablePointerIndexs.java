package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.api.PointerIndexFactory;
import com.jivesoftware.os.amza.lsm.api.PointerStream;

/**
 *
 * @author jonathan.colt
 */
public class MergeablePointerIndexs implements PointerIndex {

    // newest to oldest
    private final Object indexesLock = new Object();
    private ConcurrentReadablePointerIndex[] indexes = new ConcurrentReadablePointerIndex[0];

    public boolean merge(int count, PointerIndexFactory indexFactory) throws Exception {
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

        System.out.println("Merged (" + copy.length + ") logs.");

        synchronized (indexesLock) {
            int newSinceMerge = indexes.length - copy.length;
            DiskBackedPointerIndex[] merged = new DiskBackedPointerIndex[newSinceMerge + 1];
            System.arraycopy(indexes, copy.length, merged, 1, newSinceMerge);
            merged[0] = mergedIndex;
            indexes = merged;
        }
        for (ConcurrentReadablePointerIndex c : copy) {
            //c.destroy();
        }
        return true;
    }

    public int mergeDebut() {
        ConcurrentReadablePointerIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy.length;
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
        ConcurrentReadablePointerIndex[] copy = indexes;
        return (stream) -> {
            PointerStream found = (sortIndex, key1, timestamp, tombstoned, fp) -> {
                if (fp != -1) {
                    return stream.stream(sortIndex, key1, timestamp, tombstoned, fp);
                }
                return false;
            };
            for (ConcurrentReadablePointerIndex index : copy) {
                NextPointer feedI = index.concurrent().getPointer(key);
                if (feedI.next(found)) {
                    return false;
                }
            }
            stream.stream(Integer.MIN_VALUE, key, -1, false, -1);
            return false;
        };
    }

    @Override
    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        ConcurrentReadablePointerIndex[] copy = indexes;
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rangeScan(from, to);
        }
        return new InterleavePointerStream(feeders);
    }

    @Override
    public NextPointer rowScan() throws Exception {
        ConcurrentReadablePointerIndex[] copy = indexes;
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rowScan();
        }
        return new InterleavePointerStream(feeders);
    }

}
