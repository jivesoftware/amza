package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public class RingMemberAndHost {
    public final RingMember ringMember;
    public final RingHost ringHost;

    public RingMemberAndHost(RingMember ringMember, RingHost ringHost) {
        this.ringMember = ringMember;
        this.ringHost = ringHost;
    }

    @Override
    public String toString() {
        return "RingMemberAndHost{" +
            "ringMember=" + ringMember +
            ", ringHost=" + ringHost +
            '}';
    }
}
