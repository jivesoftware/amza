package com.jivesoftware.os.amza.storage.filer;

import com.jivesoftware.os.amza.shared.filer.IReadable;
import com.jivesoftware.os.amza.shared.filer.IWriteable;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface WALFiler extends IWriteable, IReadable {

    void eof() throws IOException;

    IReadable fileChannelFiler() throws IOException;

    IReadable fileChannelMemMapFiler(long boundaryFp) throws IOException;

}
