package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
import java.util.Arrays;
import java.util.PriorityQueue;

/**
 *
 * @author jonathan.colt
 */
class InterleavePointerStream implements PointerStream, NextPointer {
    private final NextPointer[] feeds;
    private final boolean[] done;
    private int activeFeedsI = 0;
    private final PriorityQueue<Pointer> queue = new PriorityQueue<>();

    public InterleavePointerStream(NextPointer[] feeds) throws Exception {
        this.feeds = feeds;
        this.done = new boolean[feeds.length];
        // Prime
        for (int i = 0; i < feeds.length; i++) {
            feedNext(i);
        }
    } // Prime

    private void feedNext(int i) throws Exception {
        if (!done[i]) {
            activeFeedsI = i;
            if (!feeds[i].next(this)) {
                done[i] = true; // no more in feed marker
            }
        }
    }

    @Override
    public boolean stream(int sortIndex, byte[] key, long timestamp, boolean tombstone, long version, long fp) throws Exception {
        queue.add(new Pointer(sortIndex, activeFeedsI, key, timestamp, tombstone, version, fp));
        return true;
    }

    @Override
    public boolean next(PointerStream stream) throws Exception {
        Pointer poll = queue.poll();
        if (poll != null) {
            stream.stream(poll.sortIndex, poll.key, poll.timestamps, poll.tombstones, poll.version, poll.fps);
            feedNext(poll.fi);
            Pointer peek = queue.peek();
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
}
