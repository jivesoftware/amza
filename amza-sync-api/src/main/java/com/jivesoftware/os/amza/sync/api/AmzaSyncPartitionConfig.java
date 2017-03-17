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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AmzaSyncPartitionConfig that = (AmzaSyncPartitionConfig) o;

        if (startTimestampMillis != that.startTimestampMillis) {
            return false;
        }
        return stopTimestampMillis == that.stopTimestampMillis;

    }

    @Override
    public int hashCode() {
        int result = (int) (startTimestampMillis ^ (startTimestampMillis >>> 32));
        result = 31 * result + (int) (stopTimestampMillis ^ (stopTimestampMillis >>> 32));
        return result;
    }
}
