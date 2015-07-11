package com.jivesoftware.os.amza.shared;

import java.io.IOException;

/**
 *
 */
public interface ChunkWriteable {

    void write(byte[] chunk) throws Exception;
}
