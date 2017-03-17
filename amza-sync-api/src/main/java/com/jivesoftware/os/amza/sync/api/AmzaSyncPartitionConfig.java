package com.jivesoftware.os.amza.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by jonathan.colt on 12/22/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AmzaSyncPartitionConfig {

    public final long startTimestamp;
    public final long stopTimestamp;
    public final long startVersion;
    public final long stopVersion;
    public final long timeShiftMillis;

    @JsonCreator
    public AmzaSyncPartitionConfig(@JsonProperty("startTimestamp") long startTimestamp,
        @JsonProperty("stopTimestamp") long stopTimestamp,
        @JsonProperty("startVersion") long startVersion,
        @JsonProperty("stopVersion") long stopVersion,
        @JsonProperty("timeShiftMillis") long timeShiftMillis) {
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.startVersion = startVersion;
        this.stopVersion = stopVersion;
        this.timeShiftMillis = timeShiftMillis;
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

        if (startTimestamp != that.startTimestamp) {
            return false;
        }
        if (stopTimestamp != that.stopTimestamp) {
            return false;
        }
        if (startVersion != that.startVersion) {
            return false;
        }
        if (stopVersion != that.stopVersion) {
            return false;
        }
        return timeShiftMillis == that.timeShiftMillis;

    }

    @Override
    public int hashCode() {
        int result = (int) (startTimestamp ^ (startTimestamp >>> 32));
        result = 31 * result + (int) (stopTimestamp ^ (stopTimestamp >>> 32));
        result = 31 * result + (int) (startVersion ^ (startVersion >>> 32));
        result = 31 * result + (int) (stopVersion ^ (stopVersion >>> 32));
        result = 31 * result + (int) (timeShiftMillis ^ (timeShiftMillis >>> 32));
        return result;
    }
}
