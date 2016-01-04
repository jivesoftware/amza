package com.jivesoftware.os.amza.service;

/**
 *
 */
public interface ChunkWriteable {

    void write(byte[] chunk) throws Exception;
}
