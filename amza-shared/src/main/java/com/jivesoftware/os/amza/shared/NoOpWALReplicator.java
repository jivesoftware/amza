package com.jivesoftware.os.amza.shared;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author jonathan.colt
 */
public class NoOpWALReplicator implements WALReplicator {

    @Override
    public Future<Boolean> replicate(RowsChanged rowsChanged) throws Exception {
        return new Future<Boolean>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return true;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return true;
            }

            @Override
            public Boolean get() throws InterruptedException, ExecutionException {
                return Boolean.TRUE;
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return Boolean.TRUE;
            }
        };
    }

}
