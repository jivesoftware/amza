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

}