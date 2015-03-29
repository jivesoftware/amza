package com.jivesoftware.os.amza.shared;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaRing {

    void addRingHost(String ringName, RingHost ringHost) throws Exception;

    void removeRingHost(String ringName, RingHost ringHost) throws Exception;

    List<RingHost> getRing(String ringName) throws Exception;

    void allRings(RingStream ringStream) throws Exception;

    interface RingStream {

        boolean stream(String ringName, String status, RingHost ringHost);
    }

}
