/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class NoOpWALIndex implements WALIndex {

    @Override
    public void merge(WALKeyPointers pointers, WALMergeKeyPointerStream stream) throws Exception {
        pointers.consume((WALKey key, long timestamp, boolean tombstoned, long fp) -> {
            if (stream != null) {
                if (!stream.stream(WALMergeKeyPointerStream.ignored, key, timestamp, tombstoned, fp)) {
                    return false;
                }
            }
            return true;
        });
    }

    @Override
    public void getPointer(WALKey key, WALKeyPointerStream stream) throws Exception {
        stream.stream(key, -1, false, -1);
    }

    @Override
    public void getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception {
        keyValues.consume((WALKey key, WALValue value) -> {
            return stream.stream(key, value, -1, false, -1);
        });
    }

    @Override
    public List<Boolean> containsKey(List<WALKey> key) throws Exception {
        return Collections.nCopies(key.size(), Boolean.FALSE);
    }

    @Override
    public void remove(Collection<WALKey> key) throws Exception {
    }

    @Override
    public boolean isEmpty() throws Exception {
        return false;
    }

    @Override
    public long size() throws Exception {
        return 0;
    }

    @Override
    public void commit() throws Exception {
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void compact() throws Exception {
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        return new NoOpCompactionWALIndex();
    }

    static class NoOpCompactionWALIndex implements CompactionWALIndex {

        @Override
        public void merge(WALKeyPointers pointers) throws Exception {
        }

        @Override
        public void abort() throws Exception {
        }

        @Override
        public void commit() throws Exception {
        }
    }

    @Override
    public void rowScan(WALKeyPointerStream stream) throws Exception {
    }

    @Override
    public void rangeScan(WALKey from, WALKey to, WALKeyPointerStream stream) throws Exception {
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

    @Override
    public boolean delete() throws Exception {
        return true;
    }

}
