package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IWriteable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author jonathan.colt
 */
public class IndexFilerChannelWriter implements IWriteable {

    private final IndexFile parent;
    private final FileChannel fc;
    private long fp;

    public IndexFilerChannelWriter(IndexFile parent, FileChannel fc) {
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
        fc.position(fp);
    }

    @Override
    public long length() throws IOException {
        return fc.size();
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public void close() throws IOException {
        fc.close();
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        write(ByteBuffer.wrap(b, _offset, _len));
    }

    private void write(ByteBuffer buffer) throws IOException {
        fc.position(fp);
        int size = buffer.capacity();
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        parent.addToSize(size);
        fp += size;
    }

    @Override
    public void flush(boolean fsync) throws IOException {
        fc.force(fsync);
    }

}
