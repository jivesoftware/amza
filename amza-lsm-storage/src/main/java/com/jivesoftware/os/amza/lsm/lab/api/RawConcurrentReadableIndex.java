package com.jivesoftware.os.amza.lsm.lab.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface RawConcurrentReadableIndex {

    ReadIndex reader(int bufferSize) throws Exception;

    void destroy() throws IOException;

    boolean isEmpty() throws IOException;

    long count() throws IOException;

    void close() throws IOException;

}
