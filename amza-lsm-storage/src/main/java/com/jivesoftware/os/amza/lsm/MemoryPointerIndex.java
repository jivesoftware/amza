package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.lsm.api.AppendablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.Pointer;
import com.jivesoftware.os.amza.lsm.api.PointerIndex;
import com.jivesoftware.os.amza.lsm.api.PointerStream;
import com.jivesoftware.os.amza.lsm.api.Pointers;
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
        for (Map.Entry<byte[], Pointer> e : index.entrySet()) {
            Pointer pointer = e.getValue();
            if (!stream.stream(pointer.sortIndex, e.getKey(), pointer.timestamp, pointer.tombstone, pointer.walFp)) {
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
    public PointerIndex concurrent() throws Exception {
        return new PointerIndex() {

            @Override
            public NextPointer getPointer(byte[] key) throws Exception {
                return (stream) -> {
                    Pointer pointer = index.get(key);
                    if (pointer == null) {
                        stream.stream(Integer.MIN_VALUE, key, -1, false, -1);
                    } else {
                        stream.stream(pointer.sortIndex, key, pointer.timestamp, pointer.tombstone, pointer.walFp);
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
                        Pointer pointer = next.getValue();
                        stream.stream(pointer.sortIndex, next.getKey(), pointer.timestamp, pointer.tombstone, pointer.walFp);
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
                        Pointer pointer = next.getValue();
                        stream.stream(pointer.sortIndex, next.getKey(), pointer.timestamp, pointer.tombstone, pointer.walFp);
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
