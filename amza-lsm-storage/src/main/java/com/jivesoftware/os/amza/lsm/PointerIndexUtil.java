package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.PointerStream;

/**
 *
 * @author jonathan.colt
 */
public class PointerIndexUtil {

    public static NextPointer get(ConcurrentReadablePointerIndex[] indexes, byte[] key) {
        return (stream) -> {
            PointerStream found = (sortIndex, key1, timestamp, tombstoned, version, fp) -> {
                if (fp != -1) {
                    return stream.stream(sortIndex, key1, timestamp, tombstoned, version, fp);
                }
                return false;
            };
            for (ConcurrentReadablePointerIndex index : indexes) {
                NextPointer pointer = index.concurrent(2_048).getPointer(key);
                if (pointer.next(found)) {
                    return false;
                }
            }
            stream.stream(Integer.MIN_VALUE, key, -1, false, -1, -1);
            return false;
        };
    }

    public static NextPointer rangeScan(ConcurrentReadablePointerIndex[] copy, byte[] from, byte[] to) throws Exception {
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent(4096).rangeScan(from, to);
        }
        return new InterleavePointerStream(feeders);
    }

    public static NextPointer rowScan(ConcurrentReadablePointerIndex[] copy) throws Exception {
        NextPointer[] feeders = new NextPointer[copy.length];
        for (int i = 0; i < feeders.length; i++) {
            feeders[i] = copy[i].concurrent(1024 * 1024 * 10).rowScan();
        }
        return new InterleavePointerStream(feeders);
    }
}
