package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.ring.RingMember;
import java.util.Map;

/**
 *
 */
public class Cursor {

    public final boolean taking;
    public final Map<RingMember, Long> memberTxIds;

    public Cursor(boolean taking, Map<RingMember, Long> memberTxIds) {
        this.taking = taking;
        this.memberTxIds = memberTxIds;
    }

    @Override
    public String toString() {
        return "Cursor{" +
            "taking=" + taking +
            ", memberTxIds=" + memberTxIds +
            '}';
    }
}
