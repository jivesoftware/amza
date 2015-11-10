package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;

/**
 *
 * @author jonathan.colt
 */
public class Gets implements GetRaw, RawEntryStream {

    private final ActiveScan activeScan;
    private RawEntryStream activeStream;
    private boolean found = false;

    public Gets(ActiveScan activeScan) {
        this.activeScan = activeScan;
    }

    @Override
    public boolean get(byte[] key, RawEntryStream stream) throws Exception {
        long activeFp = activeScan.getInclusiveStartOfRow(key, true);
        if (activeFp < 0) {
            return false;
        }
        activeStream = stream;
        found = false;
        activeScan.reset();
        boolean more = true;
        while (more && !found) {
            more = activeScan.next(activeFp, this);
        }
        return found;
    }

    @Override
    public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
        boolean result = activeStream.stream(rawEntry, offset, length);
        found = true;
        return result;
    }

    @Override
    public boolean result() {
        return activeScan.result();
    }

}
