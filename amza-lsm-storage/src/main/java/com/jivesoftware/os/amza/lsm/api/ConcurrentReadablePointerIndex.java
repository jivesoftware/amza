package com.jivesoftware.os.amza.lsm.api;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ConcurrentReadablePointerIndex {

    //IReadable mergeReader() throws Exception;

    PointerIndex concurrent() throws Exception;

    void destroy() throws IOException;

}
