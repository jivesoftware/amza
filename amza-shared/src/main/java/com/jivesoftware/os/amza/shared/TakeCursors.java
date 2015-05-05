package com.jivesoftware.os.amza.shared;

import java.util.List;

/**
 *
 */
public class TakeCursors {

    public final List<RingHostCursor> ringHostCursors;

    public TakeCursors(List<RingHostCursor> ringHostCursors) {
        this.ringHostCursors = ringHostCursors;
    }

    public static class RingHostCursor {

        public final RingHost ringHost;
        public final long transactionId;

        public RingHostCursor(RingHost ringHost, long transactionId) {
            this.ringHost = ringHost;
            this.transactionId = transactionId;
        }
    }
}
