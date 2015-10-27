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

    public static final byte SORT_INDEX = 4;
    public static final byte KEY_FP = 8;
    public static final byte KEY_LENGTH = 4;
    public static final byte TIMESTAMP = 8;
    public static final byte TOMBSTONE = 1;
    public static final byte VERSION = 8;
    public static final byte WAL_POINTER = 8;

    public static final int INDEX_ENTRY_SIZE = SORT_INDEX + KEY_FP + KEY_LENGTH + TIMESTAMP + TOMBSTONE + VERSION + WAL_POINTER;

    private final DiskBackedPointerIndexFiler index;
    private final DiskBackedPointerIndexFiler keys;

    public DiskBackedPointerIndex(DiskBackedPointerIndexFiler index, DiskBackedPointerIndexFiler keys) {
        this.index = index;
        this.keys = keys;
    }

    @Override
    public void destroy() throws IOException {
        // TODO aquireAll?
        close();

        new File(index.getFileName()).delete();
        new File(keys.getFileName()).delete();
    }

    @Override
    public void close() throws IOException {
        // TODO aquireAll?
        index.close();
        keys.close();
    }

    @Override
    public void append(Pointers pointers) throws Exception {
        IWriteable writeKeys = keys.fileChannelWriter();
        IWriteable writeIndex = index.fileChannelWriter();

        writeKeys.seek(0);
        writeIndex.seek(0);

        long[] keyFp = new long[]{keys.getFilePointer()};

        pointers.consume((sortIndex, key, timestamp, tombstoned, version, walPointer) -> {
            UIO.writeInt(writeIndex, sortIndex, "sortIndex");
            UIO.writeLong(writeIndex, keyFp[0], "keyFp");
            UIO.writeInt(writeIndex, key.length, "keyLength");
            UIO.writeLong(writeIndex, timestamp, "timestamp");
            UIO.writeBoolean(writeIndex, tombstoned, "tombstone");
            UIO.writeLong(writeIndex, version, "version");
            UIO.writeLong(writeIndex, walPointer, "walPointerFp");

            UIO.writeInt(writeKeys, key.length, "keyLength");
            UIO.write(writeKeys, key);
            keyFp[0] += (4 + key.length);
            return true;
        });

        writeKeys.flush(false);
        writeIndex.flush(false);

    }

    @Override
    public ReadablePointerIndex concurrent() throws Exception {
        IReadable readableIndex = index.fileChannelFiler();
        IReadable readableKeys = keys.fileChannelFiler();
        return new ReadablePointerIndex((int) (readableIndex.length() / INDEX_ENTRY_SIZE), readableIndex, readableKeys);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return index.length() == 0;
    }

    @Override
    public long count() throws IOException {
        return index.length() / INDEX_ENTRY_SIZE;
    }

    @Override
    public void flush() throws Exception {
    }
}
