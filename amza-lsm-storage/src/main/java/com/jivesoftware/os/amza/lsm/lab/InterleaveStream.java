package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;

/**
 *
 * @author jonathan.colt
 */
class InterleaveStream implements NextRawEntry {

    private final NextRawEntry[] feeds;
    private final int[] nextRawKeyLength;
    private final byte[][] nextRawEntry;
    private final int[] nextOffset;
    private final int[] nextLength;

    public InterleaveStream(NextRawEntry[] feeds) throws Exception {
        this.feeds = feeds;
        this.nextRawKeyLength = new int[feeds.length];
        this.nextRawEntry = new byte[feeds.length][];
        this.nextOffset = new int[feeds.length];
        this.nextLength = new int[feeds.length];
        for (int i = 0; i < feeds.length; i++) {
            feedNext(i);
        }
    }

    private byte[] feedNext(int i) throws Exception {
        boolean hadNext = feeds[i].next((rawEntry, offset, length) -> {
            nextRawKeyLength[i] = UIO.bytesInt(rawEntry, offset);
            nextRawEntry[i] = rawEntry;
            nextOffset[i] = offset;
            nextLength[i] = length;
            return true;
        });
        if (!hadNext) {
            nextRawEntry[i] = null;
        }
        return nextRawEntry[i];
    }

    private int streamed = -1;
    private int until = -1;

    @Override
    public boolean next(RawEntryStream stream) throws Exception {

        if (until == -1 || nextRawEntry[streamed] == null || IndexUtil.compare(nextRawEntry[streamed], 4, nextRawKeyLength[streamed],
            nextRawEntry[until], 4, nextRawKeyLength[until]) >= 0) {

            streamed = -1;
            until = -1;
            for (int i = 0; i < feeds.length; i++) {
                byte[] rawEntry = nextRawEntry[i];
                if (rawEntry == null) {
                    continue;
                }
                int rawLength = nextRawKeyLength[i];
                boolean streamedUnset = streamed == -1;
                int c = streamedUnset ? Integer.MAX_VALUE : IndexUtil.compare(rawEntry, 4, rawLength,
                    nextRawEntry[streamed], 4, nextRawKeyLength[streamed]);
                if (c == 0) {
                    rawEntry = feedNext(i);
                    rawLength = nextRawKeyLength[i];
                    if (rawEntry == null) {
                        continue;
                    }
                    c = IndexUtil.compare(rawEntry, 4, rawLength, nextRawEntry[streamed], 4, nextRawKeyLength[streamed]);
                }
                if (streamedUnset || c < 0) {
                    until = streamed;
                    streamed = i;
                } else if (until == -1 || IndexUtil.compare(rawEntry, 4, rawLength, nextRawEntry[until], 4, nextRawKeyLength[until]) < 0) {
                    until = i;
                }
            }
        }
        if (streamed != -1) {
            stream.stream(nextRawEntry[streamed],nextOffset[streamed],nextLength[streamed]);
            feedNext(streamed);
        }
        return streamed != -1;

    }
}
