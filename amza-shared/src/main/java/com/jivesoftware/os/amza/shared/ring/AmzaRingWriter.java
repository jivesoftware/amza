package com.jivesoftware.os.amza.shared.ring;

import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 * @author jonathan.colt
 */
public interface AmzaRingWriter {

    void deregister(RingMember ringMember) throws Exception;

    void register(RingMember ringMember, RingHost ringHost) throws Exception;

    void addRingMember(byte[] ringName, RingMember ringMember) throws Exception;

    void removeRingMember(byte[] ringName, RingMember ringHost) throws Exception;

}
