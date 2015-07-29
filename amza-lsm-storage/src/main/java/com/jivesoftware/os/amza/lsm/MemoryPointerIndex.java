package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author jonathan.colt
 */
public class MemoryPointerIndex implements AppendablePointerIndex, ConcurrentReadablePointerIndex, Pointers {

    private final ConcurrentSkipListMap<byte[], Pointer> index = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

    @Override
    public boolean consume(PointerStream stream) throws Exception {
        for (Map.Entry<byte[], Pointer> i : index.entrySet()) {
            Pointer walp = i.getValue();
            if (!stream.stream(walp.sortIndex, i.getKey(), walp.timestamp, walp.tombstone, walp.walFp)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void append(Pointers pointers) throws Exception {
        pointers.consume((sortIndex, key, timestamp, tombstoned, fp) -> {
            index.compute(key, (k, v) -> {
                if (v == null) {
                    return new Pointer(sortIndex, timestamp, tombstoned, fp);
                } else {
                    return v.timestamp > timestamp ? v : new Pointer(sortIndex, timestamp, tombstoned, fp);
                }
            });
            return true;
        });
    }

    @Override
    public ReadablePointerIndex concurrent() throws Exception {
        return new ReadablePointerIndex() {

            @Override
            public NextPointer getPointer(byte[] key) throws Exception {
                return (stream) -> {
                    Pointer walp = index.get(key);
                    if (walp == null) {
                        stream.stream(Integer.MIN_VALUE, key, -1, false, -1);
                    } else {
                        stream.stream(walp.sortIndex, key, walp.timestamp, walp.tombstone, walp.walFp);
                    }
                    return false;
                };
            }

            @Override
            public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
                ConcurrentNavigableMap<byte[], Pointer> subMap = index.subMap(from, true, to, false);
                Iterator<Map.Entry<byte[], Pointer>> iterator = subMap.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], Pointer> next = iterator.next();
                        Pointer walp = next.getValue();
                        stream.stream(walp.sortIndex, next.getKey(), walp.timestamp, walp.tombstone, walp.walFp);
                        return true;
                    }
                    return false;
                };
            }

            @Override
            public NextPointer rowScan() throws Exception {
                Iterator<Map.Entry<byte[], Pointer>> iterator = index.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], Pointer> next = iterator.next();
                        Pointer walp = next.getValue();
                        stream.stream(walp.sortIndex, next.getKey(), walp.timestamp, walp.tombstone, walp.walFp);
                        return true;
                    }
                    return false;
                };
            }
        };
    }

    @Override
    public void destroy() throws IOException {
        index.clear();
    }

}
