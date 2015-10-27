package com.jivesoftware.os.amza.shared.ring;

import com.jivesoftware.os.filer.io.IBA;
import java.util.Set;

/**
 *
 */
public class RingSet {

    public final long memberCacheId;
    public final Set<IBA> ringNames;

    public RingSet(long memberCacheId, Set<IBA> ringNames) {
        this.memberCacheId = memberCacheId;
        this.ringNames = ringNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RingSet ringSet = (RingSet) o;

        if (memberCacheId != ringSet.memberCacheId) {
            return false;
        }
        return !(ringNames != null ? !ringNames.equals(ringSet.ringNames) : ringSet.ringNames != null);

    }

    @Override
    public int hashCode() {
        int result = (int) (memberCacheId ^ (memberCacheId >>> 32));
        result = 31 * result + (ringNames != null ? ringNames.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RingSet{" +
            "memberCacheId=" + memberCacheId +
            ", ringNames=" + ringNames +
            '}';
    }
}
