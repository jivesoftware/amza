package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IAppendOnly;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.RawAppendableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntries;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.File;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class LeapsAndBoundsIndex implements RawConcurrentReadableIndex, RawAppendableIndex {

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte FOOTER = 2;

    private final int maxLeaps;
    private final int updatesBetweenLeaps;

    private final IndexFile index;
    
    public LeapsAndBoundsIndex(IndexFile index, int maxLeaps, int updatesBetweenLeaps) {
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
    }

    @Override
    public boolean append(RawEntries pointers) throws Exception {
        IAppendOnly writeIndex = index.fileChannelWriter(1 * 1024 * 1024);

        LeapFrog[] latestLeapFrog = new LeapFrog[1];
        int[] updatesSinceLeap = new int[1];

        byte[] lengthBuffer = new byte[4];
        long[] startOfEntryIndex = new long[updatesBetweenLeaps];
        HeapFiler entryBuffer = new HeapFiler(1024); // TODO somthing better

        byte[][] firstAndLastKey = new byte[2][];
        int[] leapCount = {0};
        long[] count = {0};

        pointers.consume((rawEntry, offset, length) -> {

            entryBuffer.reset();

            long fp = writeIndex.getFilePointer();
            startOfEntryIndex[updatesSinceLeap[0]] = fp + 1 + 4;
            UIO.writeByte(entryBuffer, ENTRY, "type");
            int entryLength = 4 + length + 4;
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);
            entryBuffer.write(rawEntry, offset, length);
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);

            writeIndex.write(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());

            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);

            if (firstAndLastKey[0] == null) {
                firstAndLastKey[0] = key;
            }
            firstAndLastKey[1] = key;
            updatesSinceLeap[0]++;
            count[0]++;

            if (updatesSinceLeap[0] >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap[0]];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap[0]);
                latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], leapCount[0], key, copyOfStartOfEntryIndex, lengthBuffer);
                updatesSinceLeap[0] = 0;
                leapCount[0]++;
            }
            return true;
        });

        if (updatesSinceLeap[0] > 0) {
            long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap[0]];
            System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap[0]);
            latestLeapFrog[0] = writeLeaps(writeIndex, latestLeapFrog[0], leapCount[0], firstAndLastKey[1], copyOfStartOfEntryIndex, lengthBuffer);
            leapCount[0]++;
        }

        UIO.writeByte(writeIndex, FOOTER, "type");
        new Footer(leapCount[0], count[0], firstAndLastKey[0], firstAndLastKey[1]).write(writeIndex, lengthBuffer);
        writeIndex.flush(false);
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

    private Leaps leaps = null;
    private TLongObjectHashMap<Leaps> leapsCache;
    private Footer footer = null;

    @Override
    public ReadIndex reader(int bufferSize) throws Exception {
        IReadable readableIndex = index.fileChannelMemMapFiler(0);
        if (readableIndex == null) {
            readableIndex = (bufferSize > 0) ? new HeapBufferedReadable(index.fileChannelFiler(), bufferSize) : index.fileChannelFiler();
        }
        byte[] lengthBuffer = new byte[8];
        if (footer == null) {
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
        return new ReadLeapsAndBoundsIndex(new ActiveScan(leaps, leapsCache, footer, readableIndex, lengthBuffer));
    }

    @Override
    public void destroy() throws IOException {
        close();
        new File(index.getFileName()).delete();
    }

    @Override
    public void close() throws IOException {
        index.close();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return index.length() == 0;
    }

    @Override
    public long count() throws IOException {
        if (footer != null) {
            return footer.count;
        }
        return 0;
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public String toString() {
        return "LeapsAndBoundsIndex{" + "index=" + index + '}';
    }

}
