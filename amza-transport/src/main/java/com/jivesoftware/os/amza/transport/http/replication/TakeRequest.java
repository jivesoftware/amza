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
package com.jivesoftware.os.amza.transport.http.replication;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;

public class TakeRequest {

    private final long highestTransactionId;
    private final RingMember taker;
    private final RingHost takerHost;
    private final PartitionName partitionName;

    @JsonCreator
    public TakeRequest(
        @JsonProperty("taker") RingMember taker,
        @JsonProperty("takerHost") RingHost takerHost,
        @JsonProperty("highestTransactionId") long highestTransactionId,
        @JsonProperty("partitionName") PartitionName partitionName) {
        this.taker = taker;
        this.takerHost = takerHost;
        this.highestTransactionId = highestTransactionId;
        this.partitionName = partitionName;
    }

    public RingMember getTaker() {
        return taker;
    }

    public RingHost getTakerHost() {
        return takerHost;
    }

    public long getHighestTransactionId() {
        return highestTransactionId;
    }

    public PartitionName getPartitionName() {
        return partitionName;
    }

    @Override
    public String toString() {
        return "TakeRequest{" + "highestTransactionId=" + highestTransactionId + ", taker=" + taker + ", partitionName=" + partitionName + '}';
    }

}
