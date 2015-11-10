package com.jivesoftware.os.amza.lsm.lab.api;

import com.jivesoftware.os.amza.lsm.lab.IndexRangeId;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface RawConcurrentReadableIndex {

    IndexRangeId id();

    ReadIndex reader(int bufferSize) throws Exception;

    void destroy() throws IOException;

    boolean isEmpty() throws IOException;

    long count() throws IOException;

    void close() throws Exception;

}
