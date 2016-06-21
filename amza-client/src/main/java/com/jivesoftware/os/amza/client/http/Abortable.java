package com.jivesoftware.os.amza.client.http;

/**
 * @author jonathan.colt
 */
public interface Abortable extends Closeable {

    void abort() throws Exception;

}
