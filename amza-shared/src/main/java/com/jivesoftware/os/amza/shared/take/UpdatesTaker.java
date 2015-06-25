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

import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

public interface UpdatesTaker {

    void streamingTakePartitionUpdates(RingMember fromRingMember,
        RingHost fromRingHost,
        long takeSessionId,
        long timeoutMillis,
        PartitionUpdatedStream updatedPartitionsStream) throws Exception;

    interface PartitionUpdatedStream {

        void update(PartitionName partitionName, long txId) throws Exception;
    }

    StreamingTakeResult streamingTakeUpdates(RingMember asRingMember,
        RingMember fromRingMember,
        RingHost fromRingHost,
        PartitionName partitionName,
        long transactionId,
        RowStream tookRowUpdates);

    class StreamingTakeResult {

        public final long partitionVersion;
        public final Throwable unreachable;
        public final Throwable error;
        public final Map<RingMember, Long> otherHighwaterMarks;

        public StreamingTakeResult(long partitionVersion,
            Exception unreachable,
            Exception error,
            Map<RingMember, Long> otherHighwaterMarks) {
            this.partitionVersion = partitionVersion;
            this.unreachable = unreachable;
            this.error = error;
            this.otherHighwaterMarks = otherHighwaterMarks;
        }
    }

    /*
     Return true if acks were acked!
     */
    boolean ackTakenUpdate(RingMember ringMember, RingHost ringHost, VersionedPartitionName versionedPartitionName, long txId);

    class AckTaken {

        public VersionedPartitionName partitionName;
        public long txId;

        public AckTaken() {
        }

        public AckTaken(VersionedPartitionName partitionName, long txId) {
            this.partitionName = partitionName;
            this.txId = txId;
        }

    }
}
