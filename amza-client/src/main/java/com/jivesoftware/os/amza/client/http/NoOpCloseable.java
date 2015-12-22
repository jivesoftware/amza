package com.jivesoftware.os.amza.client.http;

/**
 *
 * @author jonathan.colt
 */
public class NoOpCloseable implements Closeable {

    
    public NoOpCloseable() {
    }

    @Override
    public void close() throws Exception {
    }

}
