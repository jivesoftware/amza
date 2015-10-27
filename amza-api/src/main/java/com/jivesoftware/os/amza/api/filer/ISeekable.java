package com.jivesoftware.os.amza.api.filer;

import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface ISeekable {

    /**
     *
     * @return
     */
    public Object lock();

    /**
     *
     * @param position
     * @throws IOException
     */
    public void seek(long position) throws IOException;

    /**
     *
     * @return @throws IOException
     */
    public long length() throws IOException;

    /**
     *
     * @return @throws IOException
     */
    public long getFilePointer() throws IOException;
}
