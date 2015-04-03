package com.jivesoftware.os.amza.service.storage.delta;

/**
 *
 * @author jonathan.colt
 * @param <R>
 */
public class DeltaResult<R> {

    public final boolean missed;
    public final R result;

    public DeltaResult(boolean missed, R result) {
        this.missed = missed;
        this.result = result;
    }

}
