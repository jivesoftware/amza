package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.ring.RingMember;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class WALHighwater {

    public final List<RingMemberHighwater> ringMemberHighwater;

    public WALHighwater(List<RingMemberHighwater> ringMemberHighwater) {
        this.ringMemberHighwater = ringMemberHighwater;
    }

    public static class RingMemberHighwater {

        public final RingMember ringMember;
        public final long transactionId;

        public RingMemberHighwater(RingMember ringMember, long transactionId) {
            this.ringMember = ringMember;
            this.transactionId = transactionId;
        }
    }
}
