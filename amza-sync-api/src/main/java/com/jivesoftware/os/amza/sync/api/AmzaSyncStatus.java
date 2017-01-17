package com.jivesoftware.os.amza.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class AmzaSyncStatus {

    public final long timestamp;
    public final boolean taking;

    @JsonCreator
    public AmzaSyncStatus(@JsonProperty("timestamp") long timestamp,
        @JsonProperty("taking") boolean taking) {
        this.timestamp = timestamp;
        this.taking = taking;
    }
}
