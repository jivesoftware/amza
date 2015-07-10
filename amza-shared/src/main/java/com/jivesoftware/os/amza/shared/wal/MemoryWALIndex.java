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

import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryWALIndex implements WALIndex {

    private final ConcurrentSkipListMap<byte[], WALPointer> index = new ConcurrentSkipListMap<>(WALKey::compare);

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
    public boolean rowScan(WALKeyPointerStream stream) throws Exception {
        for (Entry<byte[], WALPointer> e : index.entrySet()) {
            WALPointer rowPointer = e.getValue();
            if (!stream.stream(e.getKey(), rowPointer.getTimestampId(), rowPointer.getTombstoned(), rowPointer.getFp())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, WALKeyPointerStream stream) throws Exception {
        if (to == null) {
            for (Entry<byte[], WALPointer> e : index.tailMap(from, true).entrySet()) {
                WALPointer rowPointer = e.getValue();
                if (!stream.stream(e.getKey(), rowPointer.getTimestampId(), rowPointer.getTombstoned(), rowPointer.getFp())) {
                    return false;
                }
            }
        } else {
            for (Entry<byte[], WALPointer> e : index.subMap(from, to).entrySet()) {
                WALPointer rowPointer = e.getValue();
                if (!stream.stream(e.getKey(), rowPointer.getTimestampId(), rowPointer.getTombstoned(), rowPointer.getFp())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public long size() throws Exception {
        return index.size();
    }

    @Override
    public boolean containsKeys(WALKeys keys, KeyContainedStream stream) throws Exception {
        return keys.consume(key -> {
            WALPointer got = index.get(key);
            return stream.stream(key, got != null && !got.getTombstoned());
        });
    }

    @Override
    public boolean getPointer(byte[] key, WALKeyPointerStream stream) throws Exception {
        WALPointer got = index.get(key);
        return stream(key, got, stream);
    }

    private boolean stream(byte[] key, WALPointer pointer, WALKeyPointerStream stream) throws Exception {
        if (pointer == null) {
            return stream.stream(key, -1, false, -1);
        } else {
            return stream.stream(key, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
        }
    }

    private boolean stream(byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, WALPointer pointer, WALKeyValuePointerStream stream) throws
        Exception {
        if (pointer == null) {
            return stream.stream(key, value, valueTimestamp, valueTombstoned, -1, false, -1);
        } else {
            return stream.stream(key, value, valueTimestamp, valueTombstoned, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
        }
    }

    @Override
    public boolean getPointers(WALKeys keys, WALKeyPointerStream stream) throws Exception {
        return keys.consume(key -> stream(key, index.get(key), stream));
    }

    @Override
    public boolean getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        return keyValues.consume((byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned) -> {
            WALPointer pointer = index.get(key);
            if (pointer != null) {
                return stream(key, value, valueTimestamp, valueTombstoned, pointer, stream);
            } else {
                return stream(key, value, valueTimestamp, valueTombstoned, null, stream);
            }
        });
    }

    @Override
    public boolean merge(TxKeyPointers pointers, MergeTxKeyPointerStream stream) throws Exception {
        return pointers.consume((long txId, byte[] key, long timestamp, boolean tombstoned, long fp) -> {
            return merge(txId, key, timestamp, tombstoned, fp, stream);
        });
    }

    private boolean merge(long txId, byte[] key, long timestamp, boolean tombstoned, long fp, MergeTxKeyPointerStream stream) throws Exception {
        byte[] mode = new byte[1];
        WALPointer compute = index.compute(key, (existingKey, existingPointer) -> {
            if (existingPointer == null || timestamp > existingPointer.getTimestampId()) {
                mode[0] = (existingPointer == null) ? WALMergeKeyPointerStream.added : WALMergeKeyPointerStream.clobbered;
                return new WALPointer(fp, timestamp, tombstoned);
            } else {
                mode[0] = WALMergeKeyPointerStream.ignored;
                return existingPointer;
            }
        });
        return stream.stream(mode[0], txId, key, compute.getTimestampId(), compute.getTombstoned(), compute.getFp());
    }

    @Override
    public boolean remove(WALKeys keys) throws Exception {
        return keys.consume(key -> {
            index.remove(key);
            return true;
        });
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
}
