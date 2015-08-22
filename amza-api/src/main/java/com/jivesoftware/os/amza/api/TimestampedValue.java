package com.jivesoftware.os.amza.api;

import java.util.Arrays;

/**
 *
 */
public class TimestampedValue {

    private final long timestampId;
    private final long version;
    private final byte[] value;

    public TimestampedValue(long timestampId, long version, byte[] value) {
        this.timestampId = timestampId;
        this.version = version;
        this.value = value;
    }

    public long getTimestampId() {
        return timestampId;
    }

    public long getVersion() {
        return version;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimestampedValue that = (TimestampedValue) o;

        if (timestampId != that.timestampId) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        return Arrays.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 19 * hash + (int) (this.timestampId ^ (this.timestampId >>> 32));
        hash = 19 * hash + (int) (this.version ^ (this.version >>> 32));
        hash = 19 * hash + Arrays.hashCode(this.value);
        return hash;
    }

    @Override
    public String toString() {
        return "TimestampedValue{"
            + "timestampId=" + timestampId
            + ", version=" + version
            + ", value=" + Arrays.toString(value)
            + '}';
    }
}
