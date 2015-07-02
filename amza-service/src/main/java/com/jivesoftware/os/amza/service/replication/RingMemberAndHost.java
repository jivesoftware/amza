package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
class RingMemberAndHost {
    final RingMember ringMember;
    final RingHost ringHost;

    public RingMemberAndHost(RingMember ringMember, RingHost ringHost) {
        this.ringMember = ringMember;
        this.ringHost = ringHost;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 11 * hash + Objects.hashCode(this.ringMember);
        hash = 11 * hash + Objects.hashCode(this.ringHost);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RingMemberAndHost other = (RingMemberAndHost) obj;
        if (!Objects.equals(this.ringMember, other.ringMember)) {
            return false;
        }
        if (!Objects.equals(this.ringHost, other.ringHost)) {
            return false;
        }
        return true;
    }

}
