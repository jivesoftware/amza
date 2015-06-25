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
package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import java.io.DataOutputStream;
import java.util.Set;

public interface AmzaInstance {

    Set<PartitionName> getPartitionNames();

    void destroyPartition(PartitionName partitionName) throws Exception;

    long getTimestamp(long timestamp, long millisAgo) throws Exception;

    void streamingTakePartitionUpdates(DataOutputStream dos,
         RingMember ringMember, long takeSessionId,long timeoutMillis) throws Exception;

    void streamingTakeFromPartition(DataOutputStream dos,
        RingMember ringMember,
        PartitionName partitionName,
        long highestTransactionId) throws Exception;

    void takeAcks(RingMember ringMember, RingHost ringHost, StreamableAcks acks) throws Exception;

    interface StreamableAcks {
        void stream(AcksStream acksStream) throws Exception;
    }

    interface AcksStream {

        void stream(VersionedPartitionName versionedPartitionName, long txId) throws Exception;
    }

}
