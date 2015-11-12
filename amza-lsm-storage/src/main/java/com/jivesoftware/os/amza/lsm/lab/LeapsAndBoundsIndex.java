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

    private final byte[] lengthBuffer = new byte[8];
    private final IndexRangeId id;
    private final IndexFile index;
    private final ExecutorService destroy;
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final int numBonesHidden = 1024;  // TODO config
    private final Semaphore hideABone;

    public LeapsAndBoundsIndex(ExecutorService destroy, IndexRangeId id, IndexFile index) throws Exception {
        this.destroy = destroy;
        this.id = id;
        this.index = index;
        this.hideABone = new Semaphore(numBonesHidden, true);

        long indexLength = index.length();
        if (indexLength < 4) {
            System.out.println("WTF:" + indexLength);
        }
        index.seek(indexLength - 4);
        int footerLength = UIO.readInt(index, "length", lengthBuffer);
        index.seek(indexLength - (1 + footerLength));

        int type = index.read();
        if (type != FOOTER) {
            throw new RuntimeException("Corruption! " + type + " expected " + FOOTER);
        }
        this.footer = Footer.read(index, lengthBuffer);
        index.seek(0);
    
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
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }

        try {
            IReadable readableIndex = index.fileChannelMemMapFiler(0);
            if (readableIndex == null) {
                readableIndex = (bufferSize > 0) ? new HeapBufferedReadable(index.fileChannelFiler(), bufferSize) : index.fileChannelFiler();
            }

            if (leaps == null) {
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
                leapsCache = new TLongObjectHashMap<>(footer.leapCount);

            }
            return new ReadLeapsAndBoundsIndex(disposed, hideABone, new ActiveScan(leaps, leapsCache, footer, readableIndex, lengthBuffer));
        } catch (Throwable x) {
            throw x;
        } finally {
            hideABone.release();
        }
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
        return footer.count == 0;
    }

    @Override
    public long count() throws IOException {
        return footer.count;
    }

    @Override
    public String toString() {
        return "LeapsAndBoundsIndex{" + "index=" + index + '}';
    }

}
