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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryWALIndex implements WALIndex {

    private final NavigableMap<WALKey, WALPointer> index;

    public MemoryWALIndex() {
        this(new ConcurrentSkipListMap<>());
    }

    public MemoryWALIndex(NavigableMap<WALKey, WALPointer> index) {
        this.index = index;
    }

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
    public void rowScan(WALKeyPointerStream stream) throws Exception {
        for (Entry<WALKey, WALPointer> e : index.entrySet()) {
            WALKey key = e.getKey();
            WALPointer rowPointer = e.getValue();
            if (!stream.stream(key, rowPointer.getTimestampId(), rowPointer.getTombstoned(), rowPointer.getFp())) {
                break;
            }
        }
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, WALKeyPointerStream stream) throws Exception {
        if (to == null) {
            for (Entry<WALKey, WALPointer> e : index.tailMap(from, true).entrySet()) {
                WALKey key = e.getKey();
                WALPointer rowPointer = e.getValue();
                if (!stream.stream(key, rowPointer.getTimestampId(), rowPointer.getTombstoned(), rowPointer.getFp())) {
                    break;
                }
            }
        } else {
            for (Entry<WALKey, WALPointer> e : index.subMap(from, to).entrySet()) {
                WALKey key = e.getKey();
                WALPointer rowPointer = e.getValue();
                if (!stream.stream(key, rowPointer.getTimestampId(), rowPointer.getTombstoned(), rowPointer.getFp())) {
                    break;
                }
            }
        }
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
    public List<Boolean> containsKey(List<WALKey> keys) {
        List<Boolean> contains = new ArrayList<>(keys.size());
        for (WALKey key : keys) {
            WALPointer got = index.get(key);
            contains.add(got == null ? false : !got.getTombstoned());
        }
        return contains;
    }

    @Override
    public void getPointer(WALKey key, WALKeyPointerStream stream) throws Exception {
        stream(key, index.get(key), stream);
    }

    private void stream(WALKey key, WALPointer pointer, WALKeyPointerStream stream) throws Exception {
        if (pointer == null) {
            stream.stream(key, -1, false, -1);
        } else {
            stream.stream(key, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
        }
    }

    private void stream(WALKey key, WALValue value, WALPointer pointer, WALKeyValuePointerStream stream) throws Exception {
        if (pointer == null) {
            stream.stream(key, value, -1, false, -1);
        } else {
            stream.stream(key, value, pointer.getTimestampId(), pointer.getTombstoned(), pointer.getFp());
        }
    }

    @Override
    public void getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        keyValues.consume((WALKey key, WALValue value) -> {
            WALPointer pointer = index.get(key);
            if (pointer != null) {
                stream(key, value, pointer, stream);
            } else {
                stream(key, value, null, stream);
            }
            return true;
        });
    }

    @Override
    public void merge(WALKeyPointers pointers, WALMergeKeyPointerStream stream) throws Exception {
        pointers.consume((WALKey key, long timestamp, boolean tombstoned, long fp) -> {
            merge(key, timestamp, tombstoned, fp, stream);
            return true;
        });
    }

    private void merge(WALKey key, long timestamp, boolean tombstoned, long fp, WALMergeKeyPointerStream stream) throws Exception {
        byte[] mode = new byte[1];
        WALPointer compute = index.compute(key, (WALKey existingKey, WALPointer existingPointer) -> {
            if (existingPointer == null || timestamp > existingPointer.getTimestampId()) {
                mode[0] = (existingPointer == null) ? WALMergeKeyPointerStream.added : WALMergeKeyPointerStream.clobbered;
                return new WALPointer(fp, timestamp, tombstoned);
            } else {
                mode[0] = WALMergeKeyPointerStream.ignored;
                return existingPointer;
            }
        });
        stream.stream(mode[0], key, compute.getTimestampId(), compute.getTombstoned(), compute.getFp());

    }

    @Override
    public void remove(Collection<WALKey> keys) {
        for (WALKey key : keys) {
            index.remove(key);
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {

        final MemoryWALIndex rowsIndex = new MemoryWALIndex();
        return new CompactionWALIndex() {

            @Override
            public void merge(WALKeyPointers pointers) throws Exception {
                rowsIndex.merge(pointers, null);
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
