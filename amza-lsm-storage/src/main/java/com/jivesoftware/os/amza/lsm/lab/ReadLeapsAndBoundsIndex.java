package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final AtomicBoolean disposed;
    private final Semaphore hideABone;
    private final ActiveScan activeScan;

    public ReadLeapsAndBoundsIndex(AtomicBoolean disposed, Semaphore hideABone, ActiveScan activeScan) {
        this.disposed = disposed;
        this.hideABone = hideABone;
        this.activeScan = activeScan;
    }

    @Override
    public void acquire() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            throw new IllegalStateException("Cannot acquire a bone on a disposed index.");
        }
    }

    @Override
    public void release() {
        hideABone.release();
    }

    @Override
    public GetRaw get() throws Exception {
        return new Gets(activeScan);
    }

    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
        activeScan.reset();
        long fp = activeScan.getInclusiveStartOfRow(from, false);
        if (fp < 0) {
            return (stream) -> false;
        }
        return (stream) -> {
            boolean[] once = new boolean[]{false};
            boolean more = true;
            while (!once[0] && more) {
                more = activeScan.next(fp,
                    (rawEntry, offset, length) -> {
                        int keylength = UIO.bytesInt(rawEntry, offset);
                        int c = IndexUtil.compare(rawEntry, 4, keylength, from, 0, from.length);
                        if (c >= 0) {
                            c = IndexUtil.compare(rawEntry, 4, keylength, to, 0, to.length);
                            if (c < 0) {
                                once[0] = true;
                            }
                            return c < 0 && stream.stream(rawEntry, offset, length);
                        } else {
                            return true;
                        }
                    });
            }
            return activeScan.result();
        };
    }

    @Override
    public NextRawEntry rowScan() throws Exception {
        activeScan.reset();
        return (stream) -> {
            activeScan.next(0, stream);
            return activeScan.result();
        };
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public long count() throws Exception {
        return activeScan.count();
    }

    @Override
    public boolean isEmpty() throws Exception {
        return activeScan.count() == 0;
    }

}
