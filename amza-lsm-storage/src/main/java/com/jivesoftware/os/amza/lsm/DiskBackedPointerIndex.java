package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.AppendablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.Pointers;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class DiskBackedPointerIndex implements ConcurrentReadablePointerIndex, AppendablePointerIndex {

    // sortIndex + keyFp+ keyLength + timestamp + tombstone + walPointer
    public static final int INDEX_ENTRY_SIZE = 4 + 8 + 4 + 8 + 1 + 8; 

    private final DiskBackedPointerIndexFiler index;
    private final DiskBackedPointerIndexFiler keys;

    public DiskBackedPointerIndex(DiskBackedPointerIndexFiler index, DiskBackedPointerIndexFiler keys) {
        this.index = index;
        this.keys = keys;
    }

    @Override
    public void destroy() throws IOException {
        // TODO aquireAll
        index.close();
        keys.close();

        new File(index.getFileName()).delete();
        new File(keys.getFileName()).delete();
    }

    @Override
    public void append(Pointers pointers) throws Exception {
        IWriteable writeKeys = keys.fileChannelWriter();
        IWriteable writeIndex = index.fileChannelWriter();

        writeKeys.seek(0);
        writeIndex.seek(0);

        long[] keyFp = new long[]{keys.getFilePointer()};
        pointers.consume((sortIndex, key, timestamp, tombstoned, walPointer) -> {
            UIO.writeInt(writeIndex, sortIndex, "sortIndex");
            UIO.writeLong(writeIndex, keyFp[0], "keyFp");
            UIO.writeInt(writeIndex, key.length, "keyLength");
            UIO.writeLong(writeIndex, timestamp, "timestamp");
            UIO.writeBoolean(writeIndex, tombstoned, "tombstone");
            UIO.writeLong(writeIndex, walPointer, "walPointerFp");

            UIO.write(writeKeys, key);
            keyFp[0] += key.length;

            return true;
        });

        writeKeys.flush(false);
        writeIndex.flush(false);

        System.out.println("index:" + writeIndex.length() + "bytes keys:" + writeKeys.length() + "bytes");
    }


    @Override
    public ReadablePointerIndex concurrent() throws Exception {
        IReadable readableIndex = index.fileChannelFiler();
        IReadable readableKeys = keys.fileChannelFiler();
        return new ReadablePointerIndex((int) (readableIndex.length() / INDEX_ENTRY_SIZE), readableIndex, readableKeys);
    }
}
