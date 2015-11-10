package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jivesoftware.os.amza.lsm.lab.WriteLeapsAndBoundsIndex.FOOTER;
import static com.jivesoftware.os.amza.lsm.lab.WriteLeapsAndBoundsIndex.LEAP;

/**
 * @author jonathan.colt
 */
public class LeapsAndBoundsIndex implements RawConcurrentReadableIndex {

    private final IndexRangeId id;
    private final IndexFile index;
    private final ExecutorService destroy;
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final int numBonesHidden = 1024;  // TODO config
    private final Semaphore hideABone;

    public LeapsAndBoundsIndex(ExecutorService destroy, IndexRangeId id, IndexFile index) {
        this.destroy = destroy;
        this.id = id;
        this.index = index;
        this.hideABone = new Semaphore(numBonesHidden, true);
    }

    @Override
    public IndexRangeId id() {
        return id;
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
        destroy.submit(() -> {

            hideABone.acquire(numBonesHidden);
            disposed.set(true);
            try {
                index.close();
                new File(index.getFileName()).delete();
            } finally {
                hideABone.release(numBonesHidden);
            }
            return null;
        });

    }

    @Override
    public void close() throws Exception {
        hideABone.acquire(numBonesHidden);
        try {
            index.close();
        } finally {
            hideABone.release(numBonesHidden);
        }
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
    public String toString() {
        return "LeapsAndBoundsIndex{" + "index=" + index + '}';
    }

}
