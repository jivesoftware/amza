package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public class AmzaSyncPartitionConfig {

    public final PartitionName from;
    public final PartitionName to;

    @JsonCreator
    public AmzaSyncPartitionConfig(@JsonProperty("from") PartitionName from,
        @JsonProperty("to") PartitionName to) {

        this.from = from;
        this.to = to;
    }
}
