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
    private final long count;

    public Footer(int leapCount, long count) {
        this.leapCount = leapCount;
        this.count = count;
    }

    void write(IAppendOnly writeable, byte[] lengthBuffer) throws IOException {
        int entryLength = 4 + 4 + 8 + 4;
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        UIO.writeInt(writeable, leapCount, "leapCount", lengthBuffer);
        UIO.writeLong(writeable, count, "count");
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
    }

    static Footer read(IReadable readable, byte[] lengthBuffer) throws IOException {
        int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
        int leapCount = UIO.readInt(readable, "leapCount", lengthBuffer);
        long count = UIO.readLong(readable, "count", lengthBuffer);
        if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Footer(leapCount, count);
    }

}
