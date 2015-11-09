package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class Footer {

    final int leapCount;
    final long count;
    final byte[] minKey;
    final byte[] maxKey;

    public Footer(int leapCount, long count, byte[] minKey, byte[] maxKey) {
        this.leapCount = leapCount;
        this.count = count;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    void write(IAppendOnly writeable, byte[] lengthBuffer) throws IOException {
        int entryLength = 4 + 4 + 8 + 4 + minKey.length + 4 + maxKey.length + 4;
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        UIO.writeInt(writeable, leapCount, "leapCount", lengthBuffer);
        UIO.writeLong(writeable, count, "count");
        UIO.writeByteArray(writeable, minKey, "minKey", lengthBuffer);
        UIO.writeByteArray(writeable, maxKey, "maxKey", lengthBuffer);
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
    }

    static Footer read(IReadable readable, byte[] lengthBuffer) throws IOException {
        int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
        int leapCount = UIO.readInt(readable, "leapCount", lengthBuffer);
        long count = UIO.readLong(readable, "count", lengthBuffer);
        byte[] minKey = UIO.readByteArray(readable, "minKey", lengthBuffer);
        byte[] maxKey = UIO.readByteArray(readable, "minKey", lengthBuffer);

        if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Footer(leapCount, count, minKey, maxKey);
    }

}
