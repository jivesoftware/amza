package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.ICloseable;
import com.jivesoftware.os.amza.api.filer.IReadable;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface WALFiler extends ICloseable {

    void truncate(long size) throws IOException;

    IReadable reader(IReadable current, long requiredLength, int bufferSize) throws IOException;

    IAppendOnly appender() throws IOException;

    long length() throws IOException;

    Object lock();
}
