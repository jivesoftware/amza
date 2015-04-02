package com.jivesoftware.os.amza.storage.filer;

import com.jivesoftware.os.amza.shared.filer.IFiler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author jonathan.colt
 */
public class FilerChannel implements IFiler {

    private final WALFiler parent;
    private final FileChannel fc;
    private long fp;

    public FilerChannel(WALFiler parent, FileChannel fc) {
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
    public long skip(long position) throws IOException {
        long newFP = fp + position;
        if (newFP > parent.length()) {
            throw new IndexOutOfBoundsException("FP out of bounds " + fp + " " + this);
        }
        fp = newFP;
        return fp;
    }

    @Override
    public long length() throws IOException {
        return parent.length();
    }

    @Override
    public void setLength(long len) throws IOException {
        synchronized (parent.lock()) {
            parent.setLength(len);
        }
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public void eof() throws IOException {
        synchronized (parent.lock()) {
            parent.eof();
        }
    }

    @Override
    public void flush() throws IOException {
        fc.force(false);
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

    @Override
    public void write(int b) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(new byte[] { (byte) b });
        fc.write(bb, fp);
        fp++;
    }

    @Override
    public void write(byte[] b) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(b);
        fc.write(bb, fp);
        fp += b.length;
    }

    @Override
    public void write(byte[] b, int _offset, int _len) throws IOException {
        byte[] fu = new byte[_len];
        System.arraycopy(b, _offset, fu, 0, _len);
        ByteBuffer ck = ByteBuffer.wrap(fu);
        fc.write(ck, fp);
        fp += _len;
    }

}
