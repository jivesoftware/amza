package com.jivesoftware.os.amza.storage.filer;

import com.jivesoftware.os.amza.shared.filer.IReadable;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface WALFiler {

    Object lock();

    void seek(long offsetFp) throws IOException;

    void eof() throws IOException;

    long length() throws IOException;

    IReadable fileChannelFiler() throws IOException;

    IReadable fileChannelMemMapFiler(long boundaryFp) throws IOException;

}
