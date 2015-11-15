package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.RawAppendableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntries;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public class WriteLeapsAndBoundsIndex implements RawAppendableIndex {

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte FOOTER = 2;

    private final IndexRangeId indexRangeId;
    private final IndexFile index;
    private final int maxLeaps;
    private final int updatesBetweenLeaps;

    private final byte[] lengthBuffer = new byte[4];

    private LeapFrog latestLeapFrog;
    private int updatesSinceLeap;

    private final long[] startOfEntryIndex;
    private final HeapFiler entryBuffer = new HeapFiler(1024); // TODO somthing better
    private byte[] firstKey;
    private byte[] lastKey;
    private int leapCount;
    private long count;

    private final IAppendOnly writeIndex;

    public WriteLeapsAndBoundsIndex(IndexRangeId indexRangeId, IndexFile index, int maxLeaps, int updatesBetweenLeaps) throws IOException {
        this.indexRangeId = indexRangeId;
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.startOfEntryIndex = new long[updatesBetweenLeaps];
        writeIndex = index.fileChannelWriter(1 * 1024 * 1024); // TODO config;
    }

    public IndexRangeId id() {
        return indexRangeId;
    }

    public IndexFile getIndex() {
        return index;
    }

    @Override
    public boolean append(RawEntries pointers) throws Exception {

        pointers.consume((rawEntry, offset, length) -> {

            entryBuffer.reset();

            long fp = writeIndex.getFilePointer();
            startOfEntryIndex[updatesSinceLeap] = fp + 1 + 4;
            UIO.writeByte(entryBuffer, ENTRY, "type");
            int entryLength = 4 + length + 4;
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);
            entryBuffer.write(rawEntry, offset, length);
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);

            writeIndex.write(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());

            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);

            if (firstKey == null) {
                firstKey = key;
            }
            lastKey = key;
            updatesSinceLeap++;
            count++;

            if (updatesSinceLeap >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(writeIndex, latestLeapFrog, leapCount, key, copyOfStartOfEntryIndex, lengthBuffer);
                updatesSinceLeap = 0;
                leapCount++;
            }
            return true;
        });

        return true;
    }

    private LeapFrog writeLeaps(IAppendOnly writeIndex,
        LeapFrog latest,
        int index,
        byte[] key,
        long[] startOfEntryIndex,
        byte[] lengthBuffer) throws IOException {

        Leaps nextLeaps = LeapFrog.computeNextLeaps(index, key, latest, maxLeaps, startOfEntryIndex);
        UIO.writeByte(writeIndex, LEAP, "type");
        long startOfLeapFp = writeIndex.getFilePointer();
        nextLeaps.write(writeIndex, lengthBuffer);
        return new LeapFrog(startOfLeapFp, nextLeaps);
    }

    @Override
    public void close() throws IOException {
        if (updatesSinceLeap > 0) {
            long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
            System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
            latestLeapFrog = writeLeaps(writeIndex, latestLeapFrog, leapCount, lastKey, copyOfStartOfEntryIndex, lengthBuffer);
            leapCount++;
        }

        UIO.writeByte(writeIndex, FOOTER, "type");
        new Footer(leapCount, count, firstKey, lastKey).write(writeIndex, lengthBuffer);
        writeIndex.flush(false); // TODO expose config Fsync
        index.close();
    }

    @Override
    public String toString() {
        return "WriteLeapsAndBoundsIndex{" + "indexRangeId=" + indexRangeId + ", index=" + index + ", maxLeaps=" + maxLeaps + ", updatesBetweenLeaps=" + updatesBetweenLeaps + ", count=" + count + '}';
    }

}
