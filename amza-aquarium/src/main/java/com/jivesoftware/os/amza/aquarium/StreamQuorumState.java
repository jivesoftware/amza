package com.jivesoftware.os.amza.aquarium;

/**
 *
 */
public interface StreamQuorumState {

    boolean stream(Waterline waterline) throws Exception;
}
