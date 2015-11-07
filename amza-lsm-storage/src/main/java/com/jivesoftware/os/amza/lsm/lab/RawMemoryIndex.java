package com.jivesoftware.os.amza.lsm.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.lab.api.MergeRawEntry;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import com.jivesoftware.os.amza.lsm.lab.api.ReadIndex;
import com.jivesoftware.os.amza.lsm.lab.api.GetRaw;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntries;
import com.jivesoftware.os.amza.lsm.lab.api.RawAppendableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.amza.lsm.lab.api.NextRawEntry;
import com.jivesoftware.os.amza.lsm.lab.api.RawEntryStream;

/**
 *
 * @author jonathan.colt
 */
public class RawMemoryIndex implements RawAppendableIndex, RawConcurrentReadableIndex, RawEntries {

    private final ConcurrentSkipListMap<byte[], byte[]> index = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
    private final AtomicLong approximateCount = new AtomicLong();

    private final MergeRawEntry merger;

    public RawMemoryIndex(MergeRawEntry merger) {
        this.merger = merger;
    }

    @Override
    public boolean consume(RawEntryStream stream) throws Exception {
        for (Map.Entry<byte[], byte[]> e : index.entrySet()) {
            byte[] entry = e.getValue();
            if (!stream.stream(entry, 0, entry.length)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean append(RawEntries pointers) throws Exception {
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
    public ReadIndex rawConcurrent(int bufferSize) throws Exception {
        return new ReadIndex() {

            @Override
            public GetRaw get() throws Exception {

                byte[][] lastKey = new byte[1][];
                byte[][] pointer = new byte[1][];
                return (key, stream) -> {
                    if (lastKey[0] == null || lastKey[0] != key) {
                        pointer[0] = index.get(key);
                    }
                    if (pointer[0] == null) {
                        return stream.stream(null, 0, 0);
                    } else {
                        boolean more = stream.stream(pointer[0], 0, pointer[0].length);
                        pointer[0] = null;
                        if (!more) {
                            lastKey[0] = null;
                        }
                        return more;
                    }
                };
            }

            @Override
            public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
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
            public NextRawEntry rowScan() throws Exception {
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
