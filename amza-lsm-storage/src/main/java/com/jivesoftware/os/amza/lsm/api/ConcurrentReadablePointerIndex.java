package com.jivesoftware.os.amza.lsm.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ConcurrentReadablePointerIndex {

    ReadPointerIndex concurrent() throws Exception;

    void destroy() throws IOException;

    boolean isEmpty() throws IOException;

    long count() throws IOException;

    void close() throws IOException;

}
