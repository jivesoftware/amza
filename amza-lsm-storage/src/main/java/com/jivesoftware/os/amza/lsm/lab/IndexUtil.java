package com.jivesoftware.os.amza.lsm.lab;

import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;

/**
 *
 * @author jonathan.colt
 */
public class IndexUtil {

    public static GetRaw get(RawConcurrentReadableIndex[] indexes) throws Exception {

        GetRaw[] pointGets = new GetRaw[indexes.length];
        for (int i = 0; i < pointGets.length; i++) {
            ReadIndex rawConcurrent = indexes[i].rawConcurrent(2_048);
            pointGets[i] = rawConcurrent.get();
        }

        return (key, stream) -> {
            for (GetRaw pointGet : pointGets) {
                if (pointGet.next(key, stream)) {
                    return false;
                }
            }
            stream.stream(null, 0, 0);
            return false;
        };
    }

    public static NextRawEntry rangeScan(RawConcurrentReadableIndex[] copy, byte[] from, byte[] to) throws Exception {
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(4096).rangeScan(from, to);
        }
        return new InterleaveStream(feeders);
    }

    public static NextRawEntry rowScan(RawConcurrentReadableIndex[] copy) throws Exception {
        NextRawEntry[] feeders = new NextRawEntry[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].rawConcurrent(1024 * 1024 * 10).rowScan();
        }
        return new InterleaveStream(feeders);
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
