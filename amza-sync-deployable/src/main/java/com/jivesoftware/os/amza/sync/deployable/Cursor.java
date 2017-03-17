package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.ring.RingMember;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Cursor {

    public final AtomicBoolean taking;
    public final AtomicLong maxTimestamp;
    public final AtomicLong maxVersion;
    public final Map<RingMember, Long> memberTxIds;

    public Cursor(boolean taking, long maxTimestamp, long maxVersion, Map<RingMember, Long> memberTxIds) {
        this.taking = new AtomicBoolean(taking);
        this.maxTimestamp = new AtomicLong(maxTimestamp);
        this.maxVersion = new AtomicLong(maxVersion);
        this.memberTxIds = memberTxIds;
    }

    @Override
    public String toString() {
        return "Cursor{" +
            "taking=" + taking +
            ", maxTimestamp=" + maxTimestamp +
            ", maxVersion=" + maxVersion +
            ", memberTxIds=" + memberTxIds +
            '}';
    }
}
