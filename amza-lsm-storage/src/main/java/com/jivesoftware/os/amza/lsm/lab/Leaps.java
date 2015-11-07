package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class Leaps {

    private final int index;
    final byte[] lastKey;
    final long[] fps;
    final byte[][] keys;

    public Leaps(int index, byte[] lastKey, long[] fpIndex, byte[][] keys) {
        this.index = index;
        this.lastKey = lastKey;
        this.fps = fpIndex;
        this.keys = keys;
    }

    void write(IAppendOnly writeable, byte[] lengthBuffer) throws IOException {
        int entryLength = 4 + 4 + 4 + lastKey.length + 4;
        for (int i = 0; i < fps.length; i++) {
            entryLength += 8 + 4 + keys[i].length;
        }
        entryLength += 4;
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        UIO.writeInt(writeable, index, "index", lengthBuffer);
        UIO.writeInt(writeable, lastKey.length, "lastKeyLength", lengthBuffer);
        UIO.write(writeable, lastKey, "lastKey");
        UIO.writeInt(writeable, fps.length, "fpIndexLength", lengthBuffer);
        for (int i = 0; i < fps.length; i++) {
            UIO.writeLong(writeable, fps[i], "fpIndex");
            UIO.writeByteArray(writeable, keys[i], "key", lengthBuffer);
        }
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
    }

    static Leaps read(IReadable readable, byte[] lengthBuffer) throws IOException {
        int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
        int index = UIO.readInt(readable, "index", lengthBuffer);
        int lastKeyLength = UIO.readInt(readable, "lastKeyLength", lengthBuffer);
        byte[] lastKey = new byte[lastKeyLength];
        UIO.read(readable, lastKey);
        int fpIndexLength = UIO.readInt(readable, "fpIndexLength", lengthBuffer);
        long[] fpIndex = new long[fpIndexLength];
        byte[][] keys = new byte[fpIndexLength][];
        for (int i = 0; i < fpIndexLength; i++) {
            fpIndex[i] = UIO.readLong(readable, "fpIndex", lengthBuffer);
            keys[i] = UIO.readByteArray(readable, "keyLength", lengthBuffer);
        }
        if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Leaps(index, lastKey, fpIndex, keys);
    }

}
