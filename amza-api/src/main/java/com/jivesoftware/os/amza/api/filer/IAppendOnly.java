package com.jivesoftware.os.amza.api.filer;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface IAppendOnly extends ICloseable, IFilePointer {

    void write(byte b) throws IOException;

    /**
     *
     * @param b
     * @param _offset
     * @param _len
     * @throws IOException
     */
     void write(byte b[], int _offset, int _len) throws IOException;

    /**
     *
     * @throws IOException
     */
     void flush(boolean fsync) throws IOException;
}