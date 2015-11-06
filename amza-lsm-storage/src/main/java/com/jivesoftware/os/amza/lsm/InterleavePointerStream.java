package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;

/**
 *
 * @author jonathan.colt
 */
class InterleavePointerStream implements NextPointer {

    private final NextPointer[] feeds;
    private final Pointer[] next;

    public InterleavePointerStream(NextPointer[] feeds) throws Exception {
        this.feeds = feeds;
        this.next = new Pointer[feeds.length];
        for (int i = 0; i < feeds.length; i++) {
            feedNext(i);
        }
    }

    private Pointer feedNext(int i) throws Exception {
        boolean hadNext = feeds[i].next((key, timestamp, tombstoned, version, pointer) -> {
            next[i] = new Pointer(key, timestamp, tombstoned, version, pointer);
            return true;
        });
        if (!hadNext) {
            next[i] = null;
        }
        return next[i];
    }

    private int streamed = -1;
    private int until = -1;

    @Override
    public boolean next(PointerStream stream) throws Exception {

        if (until == -1 || next[streamed] == null || next[streamed].compareTo(next[until]) >= 0) {
            streamed = -1;
            until = -1;
            for (int i = 0; i < feeds.length; i++) {
                Pointer pointer = next[i];
                if (pointer == null) {
                    continue;
                }
                boolean streamedUnset = streamed == -1;
                int c = streamedUnset ? Integer.MAX_VALUE : pointer.compareTo(next[streamed]);
                if (c == 0) {
                    pointer = feedNext(i);
                    if (pointer == null) {
                        continue;
                    }
                    c = pointer.compareTo(next[streamed]);
                }
                if (streamedUnset || c < 0) {
                    until = streamed;
                    streamed = i;
                } else if (until == -1 || pointer.compareTo(next[until]) < 0) {
                    until = i;
                }
            }
        }
        if (streamed != -1) {
            Pointer pointer = next[streamed];
            stream.stream(pointer.key, pointer.timestamps, pointer.tombstones, pointer.version, pointer.fps);
            feedNext(streamed);
        }
        return streamed != -1;

    }
}
