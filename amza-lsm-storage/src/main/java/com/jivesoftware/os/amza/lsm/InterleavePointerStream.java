package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;

/**
 *
 * @author jonathan.colt
 */
class InterleavePointerStream implements NextPointer {

    private final NextPointer[] feeds;
    private final boolean[] done;
    private final Pointer[] next;
    private int activeFeedsI = 0;
    //private final PriorityQueue<Pointer> queue = new PriorityQueue<>();

    public InterleavePointerStream(NextPointer[] feeds) throws Exception {
        this.feeds = feeds;
        this.done = new boolean[feeds.length];
        this.next = new Pointer[feeds.length];
        // Prime
        for (int i = 0; i < feeds.length; i++) {
            feedNext(i);
        }
    } // Prime

    private Pointer feedNext(int i) throws Exception {
        boolean hadNext = feeds[i].next((sortIndex, key, timestamp, tombstoned, version, pointer) -> {
            next[i] = new Pointer(sortIndex, activeFeedsI, key, timestamp, tombstoned, version, pointer);
            return true;
        });
        if (!hadNext) {
            next[i] = null;
        }
        return next[i];
    }

    int streamed = -1;
    int until = -1;

    @Override
    public boolean next(PointerStream stream) throws Exception {
//        streamed = -1;
//        for (int i = 0; i < feeds.length; i++) {
//            if (next[i] == null) {
//                continue;
//            }
//            Pointer pointer = next[i];
//            if (streamed == -1 || pointer.compareTo(next[streamed]) < 0) {
//                streamed = i;
//            }
//        }
//        if (streamed != -1) {
//            Pointer pointer = next[streamed];
//            stream.stream(pointer.sortIndex, pointer.key, pointer.timestamps, pointer.tombstones, pointer.version, pointer.fps);
//            feedNext(streamed);
//        }
//        return streamed != -1;
        
        if (until == -1 || next[streamed] == null || next[streamed].compareTo(next[until]) >= 0) {
            streamed = -1;
            until = -1;
            for (int i = 0; i < feeds.length; i++) {
                if (next[i] == null) {
                    continue;
                }
                Pointer pointer = next[i];
                if (streamed == -1 || pointer.compareTo(next[streamed]) < 0) {
                    if (streamed == -1) {
                        until = streamed;
                    }
                    streamed = i;
                } else if (until == -1 || pointer.compareTo(next[until]) < 0) {
                    until = i;
                }
            }
        }
        if (streamed != -1) {
            Pointer pointer = next[streamed];
            stream.stream(pointer.sortIndex, pointer.key, pointer.timestamps, pointer.tombstones, pointer.version, pointer.fps);
            feedNext(streamed);
        }
        return streamed != -1;

        /*
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
        return true;*/
    }

    /*

    private void feedNext(int i) throws Exception {
        if (!done[i]) {
            activeFeedsI = i;
            if (!feeds[i].next(this)) {
                done[i] = true; // no more in feed marker
            }
        }
    }
     */
}
