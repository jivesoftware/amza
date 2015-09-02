package com.jivesoftware.os.amza.aquarium;

/**
 *
 * @author jonathan.colt
 */
public interface TransitionQuorum {

    boolean transition(Waterline current, long desiredTimestamp, State state) throws Exception;
}
