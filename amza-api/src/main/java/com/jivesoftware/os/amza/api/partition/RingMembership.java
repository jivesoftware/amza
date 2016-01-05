package com.jivesoftware.os.amza.api.partition;

/**
 *
 */
public interface RingMembership {

    boolean isMemberOfRing(byte[] ringName) throws Exception;
}
