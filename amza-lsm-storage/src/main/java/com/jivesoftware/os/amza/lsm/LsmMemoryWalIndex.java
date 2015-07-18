package com.jivesoftware.os.amza.lsm;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.wal.WALKeyPointers;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author jonathan.colt
 */
public class LsmMemoryWalIndex implements AppendableWalIndex, ConcurrentReadableWalIndex, WALKeyPointers {

    private final ConcurrentSkipListMap<byte[], WALPointer> index = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

    @Override
    public boolean consume(WALKeyPointerStream stream) throws Exception {
        for (Map.Entry<byte[], WALPointer> i : index.entrySet()) {
            WALPointer walp = i.getValue();
            if (!stream.stream(i.getKey(), walp.timestamp, walp.tombstone, walp.walFp)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void append(WALKeyPointers pointerStream) throws Exception {
        pointerStream.consume((key, timestamp, tombstoned, fp) -> {
            index.compute(key, (k, v) -> {
                if (v == null) {
                    return new WALPointer(timestamp, tombstoned, fp);
                } else {
                    return v.timestamp > timestamp ? v : new WALPointer(timestamp, tombstoned, fp);
                }
            });
            return true;
        });
    }

    @Override
    public ReadableWalIndex concurrent() throws Exception {
        return new ReadableWalIndex() {

            @Override
            public FeedNext getPointer(byte[] key) throws Exception {
                return (stream) -> {
                    WALPointer walp = index.get(key);
                    if (walp == null) {
                        stream.stream(key, -1, false, -1);
                    } else {
                        stream.stream(key, walp.timestamp, walp.tombstone, walp.walFp);
                    }
                    return false;
                };
            }

            @Override
            public FeedNext rangeScan(byte[] from, byte[] to) throws Exception {
                ConcurrentNavigableMap<byte[], WALPointer> subMap = index.subMap(from, true, to, false);
                Iterator<Map.Entry<byte[], WALPointer>> iterator = subMap.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], WALPointer> next = iterator.next();
                        WALPointer walp = next.getValue();
                        stream.stream(next.getKey(), walp.timestamp, walp.tombstone, walp.walFp);
                        return true;
                    }
                    return false;
                };
            }

            @Override
            public FeedNext rowScan() throws Exception {
                Iterator<Map.Entry<byte[], WALPointer>> iterator = index.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], WALPointer> next = iterator.next();
                        WALPointer walp = next.getValue();
                        stream.stream(next.getKey(), walp.timestamp, walp.tombstone, walp.walFp);
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

    static public class WALPointer {

        final long timestamp;
        final boolean tombstone;
        final long walFp;

        public WALPointer(long timestamp, boolean tombstone, long walFp) {
            this.timestamp = timestamp;
            this.tombstone = tombstone;
            this.walFp = walFp;
        }
    }
}
