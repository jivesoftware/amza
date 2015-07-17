package com.jivesoftware.os.amza.shared;

/**
 *
 */
public interface ChunkWriteable {

    void write(byte[] chunk) throws Exception;
}
