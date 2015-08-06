package com.jivesoftware.os.amza.lsm;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ConcurrentReadablePointerIndex {

    ReadablePointerIndex concurrent() throws Exception;

    void destroy() throws IOException;

}
