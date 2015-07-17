package com.jivesoftware.os.amza.shared.ring;

/**
 * @author jonathan.colt
 */
public interface AmzaRingWriter {

    void deregister(RingMember ringMember) throws Exception;

    void register(RingMember ringMember, RingHost ringHost) throws Exception;

    void addRingMember(byte[] ringName, RingMember ringMember) throws Exception;

    void removeRingMember(byte[] ringName, RingMember ringHost) throws Exception;

}
