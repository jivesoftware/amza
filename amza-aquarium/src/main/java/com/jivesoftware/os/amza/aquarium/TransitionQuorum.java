package com.jivesoftware.os.amza.aquarium;

import com.jivesoftware.os.amza.aquarium.ReadWaterline.Waterline;

/**
 *
 * @author jonathan.colt
 */
public interface TransitionQuorum {

   
    boolean transition(Waterline current, long desiredTimestamp, State state) throws Exception;
}
