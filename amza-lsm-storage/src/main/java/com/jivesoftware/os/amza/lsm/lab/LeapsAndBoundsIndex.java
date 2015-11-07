package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntries;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.File;
import java.io.IOException;
import com.jivesoftware.os.amza.lsm.lab.api.RawAppendableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;

/**
 * @author jonathan.colt
 */
public class LeapsAndBoundsIndex implements RawConcurrentReadableIndex, RawAppendableIndex {

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte BOUND = 2;
    public static final byte FOOTER = 3;

    private final int maxLeaps;
    private final int updatesBetweenLeaps;

    private final IndexFile index;
    private byte[] minKey;
    private byte[] maxKey;

    public LeapsAndBoundsIndex(IndexFile index, int maxLeaps, int updatesBetweenLeaps) {
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
    }

    @Override
    public void destroy() throws IOException {
        // TODO aquireAll?
        close();

        new File(index.getFileName()).delete();
    }

    @Override
    public void close() throws IOException {
        // TODO aquireAll?
        index.close();
    }

    @Override
    public boolean append(RawEntries pointers) throws Exception {
        IAppendOnly writeIndex = index.fileChannelWriter(1 * 1024 * 1024 * 1024);

        LeapFrog[] latestLeapFrog = new LeapFrog[1];
        int[] updatesSinceLeap = new int[1];

        byte[] lengthBuffer = new byte[4];
        long[] boundsFps = new long[updatesBetweenLeaps];
        HeapFiler entryBuffer = new HeapFiler(1024); // TODO somthing better

        byte[][] lastKey = new byte[1][];
        int[] leapCount = {0};
        long[] count = {0};

        pointers.consume((rawEntry, offset, length) -> {

            entryBuffer.reset();
            UIO.writeByte(entryBuffer, ENTRY, "type");

            int entryLength = 4 + length + 4;
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);
            entryBuffer.write(rawEntry, offset, length);
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);

            writeIndex.write(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());

            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);

            lastKey[0] = key;
            updatesSinceLeap[0]++;
            count[0]++;

            if (updatesSinceLeap[0] >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                entryBuffer.reset();
                latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], leapCount[0], key, lengthBuffer);
                updatesSinceLeap[0] = 0;
                leapCount[0]++;
            }
            return true;
        });

        if (updatesSinceLeap[0] > 0) {
            writeIndex.write(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());
            entryBuffer.reset();
            latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], leapCount[0], lastKey[0], lengthBuffer);
            leapCount[0]++;
        }

        UIO.writeByte(writeIndex, FOOTER, "type");
        new Footer(leapCount[0], count[0]).write(writeIndex, lengthBuffer);
        writeIndex.flush(false);
        return true;
    }

    private LeapFrog writeLeaps(IAppendOnly writeIndex,
        LeapFrog latest,
        int index,
        byte[] key,
        byte[] lengthBuffer) throws IOException {

        Leaps leaps = LeapFrog.computeNextLeaps(index, key, latest, maxLeaps);
        UIO.writeByte(writeIndex, LEAP, "type");
        long startOfLeapFp = writeIndex.getFilePointer();
        leaps.write(writeIndex, lengthBuffer);
        return new LeapFrog(startOfLeapFp, leaps);
    }

    Leaps leaps = null;
    //Map<Long, Leaps> leapsCache = Maps.newHashMap();
    TLongObjectHashMap<Leaps> leapsCache;
    Footer footer = null;

    @Override
    public ReadIndex rawConcurrent(int bufferSize) throws Exception {
        IReadable readableIndex = index.fileChannelMemMapFiler(0);
        if (readableIndex == null) {
            readableIndex = (bufferSize > 0) ? new HeapBufferedReadable(index.fileChannelFiler(), bufferSize) : index.fileChannelFiler();
        }

        if (footer == null) {
            byte[] lengthBuffer = new byte[8];
            long indexLength = readableIndex.length();
            if (indexLength < 4) {
                System.out.println("WTF:" + indexLength);
            }
            readableIndex.seek(indexLength - 4);
            int footerLength = UIO.readInt(readableIndex, "length", lengthBuffer);
            readableIndex.seek(indexLength - (footerLength + 1 + 4));
            int leapLength = UIO.readInt(readableIndex, "length", lengthBuffer);

            readableIndex.seek(indexLength - (1 + leapLength + 1 + footerLength));

            int type = readableIndex.read();
            if (type != LEAP) {
                throw new RuntimeException("Corruption! " + type + " expected " + LEAP);
            }
            leaps = Leaps.read(readableIndex, lengthBuffer);

            type = readableIndex.read();
            if (type != FOOTER) {
                throw new RuntimeException("Corruption! " + type + " expected " + FOOTER);
            }
            footer = Footer.read(readableIndex, lengthBuffer);
            leapsCache = new TLongObjectHashMap<>(footer.leapCount);

        }
        return new ReadLeapAndBoundsIndex(leaps, readableIndex, leapsCache);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return index.length() == 0;
    }

    @Override
    public long count() throws IOException {
        return 0; // TODO I hate counts
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public String toString() {
        return "DiskBackedLeapPointerIndex{" + "index=" + index + ", minKey=" + minKey + ", maxKey=" + maxKey + '}';

    }

}
