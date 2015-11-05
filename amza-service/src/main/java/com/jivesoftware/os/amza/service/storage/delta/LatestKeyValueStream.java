package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.stream.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.KeyUtil;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import java.util.Arrays;
import java.util.Map;

/**
 *
 * @author jonathan.colt
 */
class LatestKeyValueStream implements KeyValueStream {

    private final DeltaPeekableElmoIterator iterator;
    private final KeyValueStream keyValueStream;
    private Map.Entry<byte[], WALValue> d;

    public LatestKeyValueStream(DeltaPeekableElmoIterator iterator, KeyValueStream keyValueStream) {
        this.iterator = iterator;
        this.keyValueStream = keyValueStream;
    }

    @Override
    public boolean stream(byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, long valueVersion) throws Exception {
        if (d == null && iterator.hasNext()) {
            d = iterator.next();
        }
        boolean[] needsKey = {true};
        byte[] pk = WALKey.compose(prefix, key);
        boolean complete = WALKey.decompose((com.jivesoftware.os.amza.shared.wal.WALKey.TxFpRawKeyValueEntryStream<java.lang.Object> txFpKeyValueStream) -> {
            while (d != null && KeyUtil.compare(d.getKey(), pk) <= 0) {
                WALValue got = d.getValue();
                if (Arrays.equals(d.getKey(), pk)) {
                    needsKey[0] = false;
                }
                if (!txFpKeyValueStream.stream(-1, -1, d.getKey(), got.getValue(), got.getTimestampId(), got.getTombstoned(), got.getVersion(), null)) {
                    return false;
                }
                if (iterator.hasNext()) {
                    d = iterator.next();
                } else {
                    iterator.eos();
                    d = null;
                    break;
                }
            }
            return true;
        },
            (txId, fp, streamPrefix, streamKey, streamValue, streamValueTimestamp, streamValueTombstoned, streamValueVersion, row) -> keyValueStream.stream(streamPrefix,
            streamKey, streamValue, streamValueTimestamp, streamValueTombstoned, streamValueVersion));
        if (!complete) {
            return false;
        } else if (needsKey[0]) {
            return keyValueStream.stream(prefix, key, value, valueTimestamp, valueTombstoned, valueVersion);
        } else {
            return true;
        }
    }

}
