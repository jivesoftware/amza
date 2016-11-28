package com.jivesoftware.os.amza.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;

/**
 *
 */
public class RingPartitionProperties {

    public final int ringSize;
    public final PartitionProperties partitionProperties;

    @JsonCreator
    public RingPartitionProperties(@JsonProperty("ringSize") int ringSize,
        @JsonProperty("partitionProperties") PartitionProperties partitionProperties) {
        this.ringSize = ringSize;
        this.partitionProperties = partitionProperties;
    }

    @Override
    public String toString() {
        return "RingPartitionProperties{" +
            "ringSize=" + ringSize +
            ", partitionProperties=" + partitionProperties +
            '}';
    }
}
