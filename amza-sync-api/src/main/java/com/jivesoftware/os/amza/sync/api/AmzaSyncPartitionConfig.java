package com.jivesoftware.os.amza.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jonathan.colt on 12/22/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AmzaSyncPartitionConfig {

    public final long startTimestampMillis;
    public final long stopTimestampMillis;

    @JsonCreator
    public AmzaSyncPartitionConfig(@JsonProperty("startTimestampMillis") long startTimestampMillis,
        @JsonProperty("stopTimestampMillis") long stopTimestampMillis) {
        this.startTimestampMillis = startTimestampMillis;
        this.stopTimestampMillis = stopTimestampMillis;
    }
}
