package com.jivesoftware.os.amza.client.http;

/**
 *
 * @author jonathan.colt
 */
public class NoOpCloseable implements Abortable {

    public NoOpCloseable() {
    }

    @Override
    public void abort() throws Exception {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public String toString() {
        return "NoOpCloseable{}";
    }
}
