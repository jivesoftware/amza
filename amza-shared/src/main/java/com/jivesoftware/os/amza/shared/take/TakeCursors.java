package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.ring.RingMember;
import java.util.List;

/**
 *
 */
public class TakeCursors {

    public final List<RingMemberCursor> ringMemberCursors;

    public TakeCursors(List<RingMemberCursor> ringMemberCursors) {
        this.ringMemberCursors = ringMemberCursors;
    }

    public static class RingMemberCursor {

        public final RingMember ringMember;
        public final long transactionId;

        public RingMemberCursor(RingMember ringMember, long transactionId) {
            this.ringMember = ringMember;
            this.transactionId = transactionId;
        }
    }
}
