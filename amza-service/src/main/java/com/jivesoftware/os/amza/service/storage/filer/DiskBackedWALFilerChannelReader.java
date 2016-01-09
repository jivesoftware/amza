package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.api.filer.IReadable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * @author jonathan.colt
 */
public class DiskBackedWALFilerChannelReader implements IReadable {

    private final DiskBackedWALFiler parent;
    private FileChannel fc;
    private volatile long fp;
    private volatile boolean closed = false;

    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

    public DiskBackedWALFilerChannelReader(DiskBackedWALFiler parent, FileChannel fc) {
        this.parent = parent;
        this.fc = fc;
    }

    @Override
    public Object lock() {
        return this;
    }

    @Override
    public void seek(long position) throws IOException {
        if (position < 0 || position > parent.length()) {
            throw new IOException("seek overflow " + position + " " + this);
        }
        fp = position;
    }

    @Override
    public long length() throws IOException {
        return parent.length();
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public int read() throws IOException {
        while (!closed) {
            try {
                singleByteBuffer.position(0);
                int read = fc.read(singleByteBuffer, fp);
                fp++;
                singleByteBuffer.position(0);
                return read != 1 ? -1 : singleByteBuffer.get();
            } catch (ClosedChannelException e) {
                fc = parent.getFileChannel();
            }
        }
        return -1;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b, _offset, _len);
        while (!closed) {
            try {
                fc.read(bb, fp);
                fp += _len;
                return _len;
            } catch (ClosedChannelException e) {
                fc = parent.getFileChannel();
                bb.position(0);
            }
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        fc.close();
    }

}
