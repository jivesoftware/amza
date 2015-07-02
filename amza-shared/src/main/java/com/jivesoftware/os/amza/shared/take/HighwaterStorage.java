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
package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.wal.WALHighwater;
import java.util.concurrent.Callable;

public interface HighwaterStorage {

    void clearRing(RingMember ringMember) throws Exception;

    void setIfLarger(RingMember ringMember, VersionedPartitionName versionedPartitionName, int updates, long highWatermark) throws Exception;

    void clear(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception;

    Long get(RingMember ringMember, VersionedPartitionName versionedPartitionName) throws Exception;

    WALHighwater getPartitionHighwater(VersionedPartitionName versionedPartitionName) throws Exception;

    void flush(Callable<Void> preFlush) throws Exception;

    boolean expunge(VersionedPartitionName versionedPartitionName) throws Exception;

}
