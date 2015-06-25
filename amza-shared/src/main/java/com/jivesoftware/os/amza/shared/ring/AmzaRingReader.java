package com.jivesoftware.os.amza.shared.ring;

import java.util.NavigableMap;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaRingReader {

    String SYSTEM_RING = "system";

    RingMember getRingMember();

    RingNeighbors getRingNeighbors(String ringName) throws Exception;

    NavigableMap<RingMember, RingHost> getRing(String ringName) throws Exception;

    int getRingSize(String ringName) throws Exception;

    void allRings(RingStream ringStream) throws Exception;

    interface RingStream {

        boolean stream(String ringName, RingMember ringMember, RingHost ringHost) throws Exception;
    }

    void getRingNames(RingMember ringMember, RingNameStream ringNameStream) throws Exception;

    interface RingNameStream {

        boolean stream(String ringName) throws Exception;
    }
}
