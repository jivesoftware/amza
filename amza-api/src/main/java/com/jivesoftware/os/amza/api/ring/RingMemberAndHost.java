package com.jivesoftware.os.amza.api.ring;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author jonathan.colt
 */
public class RingMemberAndHost {

    public final RingMember ringMember;
    public final RingHost ringHost;

    @JsonCreator
    public RingMemberAndHost(@JsonProperty("ringMember") RingMember ringMember,
        @JsonProperty("ringHost") RingHost ringHost) {
        this.ringMember = ringMember;
        this.ringHost = ringHost;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RingMemberAndHost that = (RingMemberAndHost) o;

        if (ringMember != null ? !ringMember.equals(that.ringMember) : that.ringMember != null) {
            return false;
        }
        return !(ringHost != null ? !ringHost.equals(that.ringHost) : that.ringHost != null);

    }

    @Override
    public int hashCode() {
        int result = ringMember != null ? ringMember.hashCode() : 0;
        result = 31 * result + (ringHost != null ? ringHost.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RingMemberAndHost{" +
            "ringMember=" + ringMember +
            ", ringHost=" + ringHost +
            '}';
    }
}
