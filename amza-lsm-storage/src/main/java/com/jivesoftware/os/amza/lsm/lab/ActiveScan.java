package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;
import com.jivesoftware.os.amza.lsm.lab.api.ScanFromFp;

/**
 *
 * @author jonathan.colt
 */
public class ActiveScan implements ScanFromFp {

    private final IReadable readable;
    private final byte[] lengthBuffer;
    private long activeFp = Long.MAX_VALUE;
    private byte[] entryBuffer;
    private int entryLength;

    public ActiveScan(IReadable readable, byte[] lengthBuffer) {
        this.readable = readable;
        this.lengthBuffer = lengthBuffer;
    }

    @Override
    public boolean next(long fp, RawEntryStream stream) throws Exception {
        if (activeFp == Long.MAX_VALUE || activeFp != fp) {
            activeFp = fp;
            readable.seek(fp);
        }
        int type;
        while ((type = readable.read()) >= 0) {
            if (type == LeapsAndBoundsIndex.ENTRY) {
                int length = UIO.readInt(readable, "entryLength", lengthBuffer);
                entryLength = length - 4;
                if (entryBuffer == null || entryBuffer.length < entryLength) {
                    entryBuffer = new byte[entryLength];
                }
                readable.read(entryBuffer, 0, entryLength);
                return stream.stream(entryBuffer, 0, entryLength);
            } else {
                int length = UIO.readInt(readable, "entryLength", lengthBuffer);
                readable.seek(readable.getFilePointer() + (length - 4));
            }
        }
        return type >= 0;
    }

}
