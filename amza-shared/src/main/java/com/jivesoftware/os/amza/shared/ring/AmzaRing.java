package com.jivesoftware.os.amza.shared.ring;

import java.util.NavigableMap;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaRing {

    void deregister(RingMember ringMember) throws Exception;

    void register(RingMember ringMember, RingHost ringHost) throws Exception;

    RingNeighbors getRingNeighbors(String ringName) throws Exception;

    void addRingMember(String ringName, RingMember ringMember) throws Exception;

    void removeRingMember(String ringName, RingMember ringHost) throws Exception;

    NavigableMap<RingMember, RingHost> getRing(String ringName) throws Exception;

    int getRingSize(String ringName) throws Exception;

    void allRings(RingStream ringStream) throws Exception;

    interface RingStream {

        boolean stream(String ringName, RingMember ringMember, RingHost ringHost);
    }
}
