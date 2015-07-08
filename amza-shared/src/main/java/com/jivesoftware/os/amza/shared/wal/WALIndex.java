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
import com.jivesoftware.os.amza.shared.scan.RangeScannablePointers;
import java.util.Collection;
import java.util.List;

public interface WALIndex extends RangeScannablePointers {

    void merge(WALKeyPointers pointers, WALMergeKeyPointerStream stream) throws Exception;

    void getPointer(WALKey key, WALKeyPointerStream stream) throws Exception;

    void getPointers(KeyValues keyValues, WALKeyValuePointerStream stream) throws Exception;

    List<Boolean> containsKey(List<WALKey> key) throws Exception;

    void remove(Collection<WALKey> key) throws Exception;

    boolean isEmpty() throws Exception;

    long size() throws Exception;

    /**
     * Force persistence of all changes
     * @throws java.lang.Exception
     */
    void commit() throws Exception;

    void close() throws Exception;

    void compact() throws Exception;

    CompactionWALIndex startCompaction() throws Exception;

    boolean delete() throws Exception;

    interface CompactionWALIndex {

        void merge(WALKeyPointers pointers) throws Exception;

        void abort() throws Exception;

        void commit() throws Exception;
    }

    void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors);

}
