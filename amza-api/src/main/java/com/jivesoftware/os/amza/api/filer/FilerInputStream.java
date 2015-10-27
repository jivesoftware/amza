package com.jivesoftware.os.amza.api.filer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author jonathan.colt
 */
public class FilerInputStream implements IReadable {

    private final InputStream inputStream;

    public FilerInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        readFully(b, 0, b.length);
        return b.length;
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        readFully(b, _offset, _len);
        return _len;
    }

    public final void readFully(byte b[], int off, int len) throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = 0;
        while (n < len) {
            int count = inputStream.read(b, off + n, len - n);
            if (count < 0) {
                throw new EOFException();
            }
            n += count;
        }
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public Object lock() {
        return inputStream;
    }

    @Override
    public void seek(long position) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long length() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long getFilePointer() throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }

}
