package com.jivesoftware.os.amza.service.storage.filer;

import com.jivesoftware.os.amza.shared.filer.IReadable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author jonathan.colt
 */
public class DiskBackedWALFilerChannelReader implements IReadable {

    private final DiskBackedWALFiler parent;
    private final FileChannel fc;
    private long fp;

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
        if (position > parent.length()) {
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
        ByteBuffer bb = ByteBuffer.allocate(1);
        fc.read(bb, fp);
        fp++;
        bb.position(0);
        return (int) bb.get();
    }

    @Override
    public int read(byte[] b) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b);
        fc.read(bb, fp);
        fp += b.length;
        return bb.capacity();
    }

    @Override
    public int read(byte[] b, int _offset, int _len) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(_len);
        fc.read(bb, fp);
        fp += _len;
        bb.position(0);
        System.arraycopy(bb.array(), 0, b, _offset, _len);
        return _len;
    }

    @Override
    public void close() throws IOException {
        fc.close();
    }

}
