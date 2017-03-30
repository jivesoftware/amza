package com.jivesoftware.os.amza.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class AmzaSyncStatus {

    public final long timestamp;
    public final long maxTimestamp;
    public final long maxVersion;
    public final boolean exists;
    public final boolean taking;

    @JsonCreator
    public AmzaSyncStatus(@JsonProperty("timestamp") long timestamp,
        @JsonProperty("maxTimestamp") long maxTimestamp,
        @JsonProperty("maxVersion") long maxVersion,
        @JsonProperty("exists") boolean exists,
        @JsonProperty("taking") boolean taking) {
        this.timestamp = timestamp;
        this.maxTimestamp = maxTimestamp;
        this.maxVersion = maxVersion;
        this.exists = exists;
        this.taking = taking;
    }
}
