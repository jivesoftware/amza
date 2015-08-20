/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.shared.wal;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.stream.KeyContainedStream;
import com.jivesoftware.os.amza.shared.stream.KeyValuePointerStream;
import com.jivesoftware.os.amza.shared.stream.KeyValues;
import com.jivesoftware.os.amza.shared.stream.MergeTxKeyPointerStream;
import com.jivesoftware.os.amza.shared.stream.TxFpStream;
import com.jivesoftware.os.amza.shared.stream.TxKeyPointers;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.shared.stream.WALKeyPointerStream;
import com.jivesoftware.os.amza.shared.stream.WALKeyPointers;
import com.jivesoftware.os.amza.shared.stream.WALMergeKeyPointerStream;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryWALIndex implements WALIndex {

    private final ConcurrentSkipListMap<byte[], WALPointer> index = new ConcurrentSkipListMap<>(KeyUtil::compare);
    private final ConcurrentSkipListMap<byte[], ConcurrentSkipListMap<Long, ConcurrentLinkedQueue<Long>>> prefixFpIndex = new ConcurrentSkipListMap<>(
        UnsignedBytes.lexicographicalComparator());

    @Override
    public void commit() {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void compact() {

    }

    @Override
    public boolean takePrefixUpdatesSince(byte[] prefix, long sinceTransactionId, TxFpStream txFpStream) throws Exception {
        ConcurrentSkipListMap<Long, ConcurrentLinkedQueue<Long>> prefixMap = prefixFpIndex.get(prefix);
        if (prefixMap != null) {
            ConcurrentNavigableMap<Long, ConcurrentLinkedQueue<Long>> txIdMap = prefixMap.tailMap(sinceTransactionId, false);
            for (Entry<Long, ConcurrentLinkedQueue<Long>> e : txIdMap.entrySet()) {
                long txId = e.getKey();
                for (Long fp : e.getValue()) {
                    if (!txFpStream.stream(txId, fp)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        return WALKey.decompose(
            keyEntryStream -> {
                for (Entry<byte[], WALPointer> e : index.entrySet()) {
                    WALPointer rowPointer = e.getValue();
                    if (!keyEntryStream.stream(-1, rowPointer.getFp(), e.getKey(), null, rowPointer.getTimestampId(), rowPointer.getTombstoned(), null)) {
                        return false;
                    }
                }
                return true;
            },
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry) -> stream.stream(prefix, key, valueTimestamp, valueTombstoned, fp));
    }

    @Override
    public boolean rangeScan(byte[] fromPrefix, byte[] fromKey, byte[] toPrefix, byte[] toKey, WALKeyPointerStream stream) throws Exception {
        byte[] fromPk = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
        byte[] toPk = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
        return WALKey.decompose(
            keyEntryStream -> {
                for (Entry<byte[], WALPointer> e : subMap(index, fromPk, toPk).entrySet()) {
                    WALPointer rowPointer = e.getValue();
                    if (!keyEntryStream.stream(-1, rowPointer.getFp(), e.getKey(), null, rowPointer.getTimestampId(), rowPointer.getTombstoned(), null)) {
                        return false;
                    }
                }
                return true;
            },
            (txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry) -> stream.stream(prefix, key, valueTimestamp, valueTombstoned, fp));
    }

    private static ConcurrentNavigableMap<byte[], WALPointer> subMap(ConcurrentSkipListMap<byte[], WALPointer> index, byte[] from, byte[] to) {
        if (from != null && to != null) {
            if (KeyUtil.compare(from, to) <= 0) {
                return index.subMap(from, to);
            } else {
                return index.subMap(from, to).descendingMap();
            }
        } else if (from != null) {
            return index.tailMap(from, true);
        } else if (to != null) {
            return index.headMap(to, false);
        } else {
            return index;
        }
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public long deltaCount(WALKeyPointers keyPointers) throws Exception {
        long[] delta = new long[1];
        boolean completed = keyPointers.consume((prefix, key, requestTimestamp, requestTombstoned, fp) -> {
            byte[] pk = WALKey.compose(prefix, key);
            WALPointer got = index.get(pk);
            long indexFp = got != null ? got.getFp() : -1;
            boolean indexTombstoned = got != null && got.getTombstoned();

            // indexFp, indexTombstoned, requestTombstoned, delta
            // -1       false            false              1
            // -1       false            true               0
            //  1       false            false              0
            //  1       false            true               -1
            //  1       true             false              1
            //  1       true             true               0
            if (!requestTombstoned && (indexFp == -1 && !indexTombstoned || indexFp != -1 && indexTombstoned)) {
                delta[0]++;
            } else if (indexFp != -1 && !indexTombstoned && requestTombstoned) {
                delta[0]--;
            }
            return true;
        });
        if (!completed) {
            return -1;
        }
        return delta[0];
    }

    @Override
    public long size() throws Exception {
        return index.size();
    }

    @Override
    public boolean containsKeys(byte[] prefix, UnprefixedWALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume((key) -> {
            byte[] pk = WALKey.compose(prefix, key);
            WALPointer got = index.get(pk);
            return stream.stream(prefix, key, got != null && !got.getTombstoned());
        });
    }

    @Override
    public boolean getPointer(byte[] prefix, byte[] key, WALKeyPointerStream stream) throws Exception {
        byte[] pk = WALKey.compose(prefix, key);
        WALPointer got = index.get(pk);
        return stream(prefix, key, got, stream);
    }

    private boolean stream(byte[] prefix, byte[] key, WALPointer pointer, WALKeyPointerStream stream) throws Exception {
        if (pointer == null) {
            return stream.stream(prefix, key, -1, false, -1);
        } else {
            return stream.stream(prefix, key, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
        }
    }

    private boolean stream(byte[] prefix,
        byte[] key,
        byte[] value,
        long valueTimestamp,
        boolean valueTombstoned,
        WALPointer pointer,
        KeyValuePointerStream stream) throws Exception {
        if (pointer == null) {
            return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, -1, false, -1);
        } else {
            return stream.stream(prefix, key, value, valueTimestamp, valueTombstoned, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
        }
    }

    @Override
    public boolean getPointers(byte[] prefix, UnprefixedWALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume((key) -> stream(prefix, key, index.get(WALKey.compose(prefix, key)), stream));
    }

    @Override
    public boolean getPointers(KeyValues keyValues, KeyValuePointerStream stream) throws Exception {
        return keyValues.consume((prefix, key, value, valueTimestamp, valueTombstoned) -> {
            WALPointer pointer = index.get(WALKey.compose(prefix, key));
            if (pointer != null) {
                return stream(prefix, key, value, valueTimestamp, valueTombstoned, pointer, stream);
            } else {
                return stream(prefix, key, value, valueTimestamp, valueTombstoned, null, stream);
            }
        });
    }

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        return pointers.consume((txId, prefix, key, timestamp, tombstoned, fp) ->
            merge(txId, prefix, key, timestamp, tombstoned, fp, stream));
    }

    private boolean merge(long txId, byte[] prefix, byte[] key, long timestamp, boolean tombstoned, long fp, MergeTxKeyPointerStream stream) throws Exception {
        byte[] mode = new byte[1];
        WALPointer compute = index.compute(WALKey.compose(prefix, key), (existingKey, existingPointer) -> {
            if (existingPointer == null || timestamp > existingPointer.getTimestampId()) {
                mode[0] = (existingPointer == null) ? WALMergeKeyPointerStream.added : WALMergeKeyPointerStream.clobbered;
                return new WALPointer(fp, timestamp, tombstoned);
            } else {
                mode[0] = WALMergeKeyPointerStream.ignored;
                return existingPointer;
            }
        });
        if (prefix != null) {
            ConcurrentSkipListMap<Long, ConcurrentLinkedQueue<Long>> prefixMap = prefixFpIndex.computeIfAbsent(prefix, bytes -> new ConcurrentSkipListMap<>());
            ConcurrentLinkedQueue<Long> queue = prefixMap.computeIfAbsent(txId, _txId -> new ConcurrentLinkedQueue<>());
            queue.add(fp);
        }
        if (stream != null) {
            return stream.stream(mode[0], txId, prefix, key, compute.getTimestampId(), compute.getTombstoned(), compute.getFp());
        } else {
            return true;
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {

        final MemoryWALIndex rowsIndex = new MemoryWALIndex();
        return new CompactionWALIndex() {

            @Override
            public boolean merge(TxKeyPointers pointers) throws Exception {
                return rowsIndex.merge(pointers, null);
            }

            @Override
            public void abort() throws Exception {
            }

            @Override
            public void commit() throws Exception {
                index.clear();
                index.putAll(rowsIndex.index);
            }
        };

    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {

    }

    @Override
    public boolean delete() throws Exception {
        index.clear();
        return true;
    }

    private static class TxFp {
        private final long txId;
        private final long[] fps;

        public TxFp(long txId, long[] fps) {
            this.txId = txId;
            this.fps = fps;
        }
    }
}
