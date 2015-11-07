package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.RawConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import com.jivesoftware.os.amza.lsm.api.RawPointGet;
import com.jivesoftware.os.amza.lsm.api.RawReadPointerIndex;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexUtil {

    public static RawPointGet get(RawConcurrentReadablePointerIndex[] indexes) throws Exception {

        RawPointGet[] pointGets = new RawPointGet[indexes.length];
        for (int i = 0; i < pointGets.length; i++) {
            RawReadPointerIndex rawConcurrent = indexes[i].rawConcurrent(2_048);
            pointGets[i] = rawConcurrent.getPointer();
        }

        return (key, stream) -> {
            for (RawPointGet pointGet : pointGets) {
                if (pointGet.next(key, stream)) {
                    return false;
                }
            }
            stream.stream(null, 0, 0);
            return false;
        };
    }

    public static RawNextPointer rangeScan(RawConcurrentReadablePointerIndex[] copy, byte[] from, byte[] to) throws Exception {
        RawNextPointer[] feeders = new RawNextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(4096).rangeScan(from, to);
        }
        return new InterleavePointerStream(feeders);
    }

    public static RawNextPointer rowScan(RawConcurrentReadablePointerIndex[] copy) throws Exception {
        RawNextPointer[] feeders = new RawNextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(1024 * 1024 * 10).rowScan();
        }
        return new InterleavePointerStream(feeders);
    }

    public static int compare(byte[] left, int leftOffset, int leftLength, byte[] right, int rightOffset, int rightLength) {
        int minLength = Math.min(leftLength, rightLength);
        for (int i = 0; i < minLength; i++) {
            int result = (left[leftOffset + i] & 0xFF) - (right[rightOffset + i] & 0xFF);
            if (result != 0) {
                return result;
            }
        }
        return leftLength - rightLength;
    }
}
