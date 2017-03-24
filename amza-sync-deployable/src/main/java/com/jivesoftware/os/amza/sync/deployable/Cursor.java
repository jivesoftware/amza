package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.ring.RingMember;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class Cursor {

    public final boolean taking;
    public final long maxTimestamp;
    public final long maxVersion;
    public final Map<RingMember, Long> memberTxIds;

    public Cursor(boolean taking, long maxTimestamp, long maxVersion, Map<RingMember, Long> memberTxIds) {
        this.taking = taking;
        this.maxTimestamp = maxTimestamp;
        this.maxVersion = maxVersion;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Cursor cursor = (Cursor) o;

        if (taking != cursor.taking) {
            return false;
        }
        if (maxTimestamp != cursor.maxTimestamp) {
            return false;
        }
        if (maxVersion != cursor.maxVersion) {
            return false;
        }
        return memberTxIds != null ? memberTxIds.equals(cursor.memberTxIds) : cursor.memberTxIds == null;

    }

    @Override
    public int hashCode() {
        int result = (taking ? 1 : 0);
        result = 31 * result + (int) (maxTimestamp ^ (maxTimestamp >>> 32));
        result = 31 * result + (int) (maxVersion ^ (maxVersion >>> 32));
        result = 31 * result + (memberTxIds != null ? memberTxIds.hashCode() : 0);
        return result;
    }
}
