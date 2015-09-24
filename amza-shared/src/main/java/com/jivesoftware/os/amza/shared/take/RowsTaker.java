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

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import java.util.Map;

public interface RowsTaker {

    StreamingRowsResult rowsStream(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        VersionedPartitionName remoteVersionedPartitionName,
        long remoteTxId,
        long localLeadershipToken,
        RowStream rowStream);

    class StreamingRowsResult {

        public final Throwable unreachable;
        public final Throwable error;
        public final long leadershipToken;
        public final long partitionVersion;
        public final Map<RingMember, Long> otherHighwaterMarks;

        public StreamingRowsResult(Exception unreachable,
            Exception error,
            long leadershipToken,
            long partitionVersion,
            Map<RingMember, Long> otherHighwaterMarks) {
            this.unreachable = unreachable;
            this.error = error;
            this.leadershipToken = leadershipToken;
            this.partitionVersion = partitionVersion;
            this.otherHighwaterMarks = otherHighwaterMarks;
        }
    }

    boolean rowsTaken(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        long takeSessionId,
        VersionedPartitionName versionedPartitionName,
        long txId,
        long localLeadershipToken);

}
