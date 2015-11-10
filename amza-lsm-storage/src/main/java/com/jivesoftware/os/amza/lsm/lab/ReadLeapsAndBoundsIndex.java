package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;

/**
 * @author jonathan.colt
 */
public class ReadLeapsAndBoundsIndex implements ReadIndex {

    private final ActiveScan activeScan;
   
    public ReadLeapsAndBoundsIndex(ActiveScan activeScan) {
        this.activeScan = activeScan;
    }

    @Override
    public GetRaw get() throws Exception {
        return new Gets(activeScan);
    }

    
    @Override
    public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
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
