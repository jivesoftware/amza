package com.jivesoftware.os.amza.aquarium;

import java.util.concurrent.Callable;

/**
 *
 */
public interface AwaitLivelyEndState {

    State awaitChange(Callable<State> awaiter, long timeoutMillis) throws Exception;

    void notifyChange(Callable<Boolean> change) throws Exception;
}