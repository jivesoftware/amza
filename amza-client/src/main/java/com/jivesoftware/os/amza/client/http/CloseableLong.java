package com.jivesoftware.os.amza.client.http;

/**
 *
 * @author jonathan.colt
 */
public class CloseableLong implements Abortable {

    private final long l;

    public CloseableLong(long l) {
        this.l = l;
    }

    long getLong() {
        return l;
    }

    @Override
    public void abort() throws Exception {
    }

    @Override
    public void close() throws Exception {
    }
}
