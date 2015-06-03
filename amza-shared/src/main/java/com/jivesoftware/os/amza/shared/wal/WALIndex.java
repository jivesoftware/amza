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

import com.jivesoftware.os.amza.shared.region.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.region.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.scan.RangeScannable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface WALIndex extends RangeScannable<WALPointer> {

    void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entry) throws Exception;

    WALPointer getPointer(WALKey key) throws Exception;

    WALPointer[] getPointers(WALKey[] keys) throws Exception;

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

        void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entry) throws Exception;

        void abort() throws Exception;

        void commit() throws Exception;
    }

    void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors);

}
