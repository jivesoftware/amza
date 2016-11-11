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
package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import java.io.DataOutputStream;

public interface AmzaInstance {

    Iterable<PartitionName> getAllPartitionNames() throws Exception;

    Iterable<PartitionName> getMemberPartitionNames() throws Exception;

    Iterable<PartitionName> getSystemPartitionNames() throws Exception;

    void destroyPartition(PartitionName partitionName) throws Exception;

    long getTimestamp(long timestamp, long millisAgo) throws Exception;

    void availableRowsStream(boolean system,
        ChunkWriteable writeable,
        RingMember remoteRingMember,
        TimestampedRingHost remoteTimestampedRingHost,
        long takeSessionId,
        long timeoutMillis) throws Exception;

    void rowsStream(DataOutputStream dos,
        RingMember remoteRingMember,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId,
        long leadershipToken) throws Exception;

    void rowsTaken(RingMember remoteRingMember,
        long takeSessionId,
        VersionedPartitionName localVersionedPartitionName,
        long localTxId,
        long leadershipToken) throws Exception;

    void pong(RingMember remoteRingMember, long takeSessionId) throws Exception;

    void invalidate(RingMember ringMember, long takeSessionId, VersionedPartitionName versionedPartitionName) throws Exception;
}
