package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.MergeRawEntry;
import com.jivesoftware.os.amza.lsm.api.RawAppendablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.RawNextPointer;
import com.jivesoftware.os.amza.lsm.api.RawPointerStream;
import com.jivesoftware.os.amza.lsm.api.RawPointers;
import com.jivesoftware.os.amza.lsm.api.RawReadPointerIndex;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class RawMemoryPointerIndex implements RawAppendablePointerIndex, RawConcurrentReadablePointerIndex, RawPointers {

    private final ConcurrentSkipListMap<byte[], byte[]> index = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    private final AtomicLong approximateCount = new AtomicLong();

    private final MergeRawEntry merger;

    public RawMemoryPointerIndex(MergeRawEntry merger) {
        this.merger = merger;
    }

    @Override
    public boolean consume(RawPointerStream stream) throws Exception {
        for (Map.Entry<byte[], byte[]> e : index.entrySet()) {
            byte[] entry = e.getValue();
            if (!stream.stream(entry, 0, entry.length)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean append(RawPointers pointers) throws Exception {
        return pointers.consume((rawEntry, offset, length) -> {
            int keyLength = UIO.bytesInt(rawEntry);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);
            index.compute(key, (k, v) -> {
                if (v == null) {
                    approximateCount.incrementAndGet();
                    return rawEntry;
                } else {
                    return merger.merge(v, rawEntry);
                }
            });
            return true;
        });
    }

    @Override
    public RawReadPointerIndex rawConcurrent(int bufferSize) throws Exception {
        return new RawReadPointerIndex() {

            @Override
            public RawNextPointer getPointer(byte[] key) throws Exception {

                byte[][] pointer = new byte[1][];
                pointer[0] = index.get(key);
                return (stream) -> {
                    if (pointer[0] == null) {
                        return stream.stream(null, 0, 0);
                    } else {
                        boolean done = stream.stream(pointer[0], 0, pointer[0].length);
                        pointer[0] = null;
                        return done;
                    }
                };
            }

            @Override
            public RawNextPointer rangeScan(byte[] from, byte[] to) throws Exception {
                ConcurrentNavigableMap<byte[], byte[]> subMap = index.subMap(from, true, to, false);
                Iterator<Map.Entry<byte[], byte[]>> iterator = subMap.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        stream.stream(next.getValue(), 0, next.getValue().length);
                        return true;
                    }
                    return false;
                };
            }

            @Override
            public RawNextPointer rowScan() throws Exception {
                Iterator<Map.Entry<byte[], byte[]>> iterator = index.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        stream.stream(next.getValue(), 0, next.getValue().length);
                        return true;
                    }
                    return false;
                };
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public long count() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean isEmpty() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    @Override
    public void destroy() throws IOException {
        approximateCount.set(0);
        index.clear();
    }

    @Override
    public void close() {
        approximateCount.set(0);
        index.clear();
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public long count() {
        return approximateCount.get();
    }

    @Override
    public void commit() throws Exception {
    }

}
