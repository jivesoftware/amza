package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface WALFiler extends IWriteable {

    void eof() throws IOException;

    IReadable fileChannelFiler() throws IOException;

    IReadable bestFiler(IReadable current, long boundaryFp) throws IOException;

}
