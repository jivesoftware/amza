package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.PriorityQueue;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexs {

    // most newest to oldest
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

        PointerIndex mergedIndex = indexFactory.createPointerIndex();
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rowScan();
        }
        FeedInterleaver feedInterleaver = new FeedInterleaver(feeders);
        mergedIndex.append((stream) -> {
            while (feedInterleaver.next(stream));
            return true;
        });

        System.out.println("Merged (" + copy.length + ") logs.");

        synchronized (indexesLock) {
            int newSinceMerge = indexes.length - copy.length;
            PointerIndex[] merged = new PointerIndex[newSinceMerge + 1];
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

    public void append(ConcurrentReadablePointerIndex walIndex) {
        synchronized (indexesLock) {
            ConcurrentReadablePointerIndex[] append = new ConcurrentReadablePointerIndex[indexes.length + 1];
            append[0] = walIndex;
            System.arraycopy(indexes, 0, append, 1, indexes.length);
            indexes = append;
        }
    }

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

    public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
        ConcurrentReadablePointerIndex[] copy = indexes;
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rangeScan(from, to);
        }
        return new FeedInterleaver(feeders);
    }

    public NextPointer rowScan() throws Exception {
        ConcurrentReadablePointerIndex[] copy = indexes;
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rowScan();
        }
        return new FeedInterleaver(feeders);
    }

    static class FeedInterleaver implements PointerStream, NextPointer {

        private final NextPointer[] feeds;
        private final boolean[] done;
        private int activeFeedsI = 0;
        private final PriorityQueue<WALPointer> queue = new PriorityQueue<>();

        public FeedInterleaver(NextPointer[] feeds) throws Exception {
            this.feeds = feeds;
            this.done = new boolean[feeds.length];
            // Prime
            for (int i = 0; i < feeds.length; i++) {
                feedNext(i);
            }
        }

        private void feedNext(int i) throws Exception {
            if (!done[i]) {
                activeFeedsI = i;
                if (!feeds[i].next(this)) {
                    done[i] = true; // no more in feed marker
                }
            }
        }

        @Override
        public boolean stream(int sortIndex, byte[] key, long timestamp, boolean tombstone, long fp) throws Exception {
            queue.add(new WALPointer(sortIndex, activeFeedsI, key, timestamp, tombstone, fp));
            return true;
        }

        @Override
        public boolean next(PointerStream stream) throws Exception {

            WALPointer poll = queue.poll();
            if (poll != null) {
                stream.stream(poll.sortIndex, poll.key, poll.timestamps, poll.tombstones, poll.fps);
                feedNext(poll.fi);
                WALPointer peek = queue.peek();
                while (peek != null && Arrays.equals(peek.key, poll.key)) {
                    feedNext(queue.poll().fi);
                    peek = queue.peek();
                }
            } else {
                return activeFeedsI != -1;
            }

            if (activeFeedsI > -1) {
                activeFeedsI = -1;
                for (int i = 0; i < feeds.length; i++) {
                    feedNext(i);
                }
            }
            return true;
        }

        static class WALPointer implements Comparable<WALPointer> {

            final int sortIndex;
            final int fi;
            final byte[] key;
            final long timestamps;
            final boolean tombstones;
            final long fps;

            public WALPointer(int sortIndex, int fi, byte[] key, long timestamps, boolean tombstones, long fps) {
                this.sortIndex = sortIndex;
                this.fi = fi;
                this.key = key;
                this.timestamps = timestamps;
                this.tombstones = tombstones;
                this.fps = fps;
            }

            @Override
            public int compareTo(WALPointer o) {
                int i = UnsignedBytes.lexicographicalComparator().compare(key, o.key);
                if (i != 0) {
                    return i;
                }
                return -Long.compare(fps, o.fps);
            }
        }
    }
}
