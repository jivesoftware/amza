package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import java.util.Arrays;
import java.util.PriorityQueue;

/**
 *
 * @author jonathan.colt
 */
public class LsmWalIndexs {

    // most newest to oldest
    private final Object indexesLock = new Object();
    private ConcurrentReadableWalIndex[] indexes = new ConcurrentReadableWalIndex[0];

    public void merge(LsmWalIndexFactory indexFactory) throws Exception {
        ConcurrentReadableWalIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        LsmWalIndex mergedIndex = indexFactory.createWALIndex();
        FeedNext[] feeders = new FeedNext[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rowScan();
        }
        FeedInterleaver feedInterleaver = new FeedInterleaver(feeders);
        mergedIndex.append((stream) -> {
            while (feedInterleaver.feedNext(stream));
            return true;
        });

        synchronized (indexesLock) {
            int keep = indexes.length - copy.length;
            LsmWalIndex[] merged = new LsmWalIndex[keep + 1];
            System.arraycopy(indexes, 0, merged, 0, keep);
            merged[keep] = mergedIndex;
            indexes = merged;
        }
        for (ConcurrentReadableWalIndex c : copy) {
            c.destroy();
        }
    }

    public void append(ConcurrentReadableWalIndex walIndex) {
        synchronized (indexesLock) {
            ConcurrentReadableWalIndex[] append = new ConcurrentReadableWalIndex[indexes.length + 1];
            append[0] = walIndex;
            System.arraycopy(indexes, 0, append, 1, indexes.length);
            indexes = append;
        }
    }

    public FeedNext getPointer(byte[] key) throws Exception {
        ConcurrentReadableWalIndex[] copy = indexes;
        return (stream) -> {
            WALKeyPointerStream found = (key1, timestamp, tombstoned, fp) -> {
                if (fp != -1) {
                    return stream.stream(key1, timestamp, tombstoned, fp);
                }
                return false;
            };
            for (ConcurrentReadableWalIndex index : copy) {
                FeedNext feedI = index.concurrent().getPointer(key);
                if (feedI.feedNext(found)) {
                    return false;
                }
            }
            stream.stream(key, -1, false, -1);
            return false;
        };
    }

    public FeedNext rangeScan(byte[] from, byte[] to) throws Exception {
        ConcurrentReadableWalIndex[] copy = indexes;
        FeedNext[] feeders = new FeedNext[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rangeScan(from, to);
        }
        return new FeedInterleaver(feeders);
    }

    public FeedNext rowScan() throws Exception {
        ConcurrentReadableWalIndex[] copy = indexes;
        FeedNext[] feeders = new FeedNext[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent().rowScan();
        }
        return new FeedInterleaver(feeders);
    }

    static class FeedInterleaver implements WALKeyPointerStream, FeedNext {

        private final FeedNext[] feeds;
        private final boolean[] done;
        private int activeFeedsI = 0;
        private final PriorityQueue<WALPointer> queue = new PriorityQueue<>();

        public FeedInterleaver(FeedNext[] feeds) throws Exception {
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
                if (!feeds[i].feedNext(this)) {
                    done[i] = true; // no more in feed marker
                }
            }
        }

        @Override
        public boolean stream(byte[] key, long timestamp, boolean tombstone, long fp) throws Exception {
            queue.add(new WALPointer(activeFeedsI, key, timestamp, tombstone, fp));
            return true;
        }

        @Override
        public boolean feedNext(WALKeyPointerStream pointerStream) throws Exception {

            WALPointer poll = queue.poll();
            if (poll != null) {
                pointerStream.stream(poll.key, poll.timestamps, poll.tombstones, poll.fps);
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

            final int fi;
            final byte[] key;
            final long timestamps;
            final boolean tombstones;
            final long fps;

            public WALPointer(int fi, byte[] key, long timestamps, boolean tombstones, long fps) {
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
