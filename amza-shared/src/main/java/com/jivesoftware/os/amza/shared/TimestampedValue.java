package com.jivesoftware.os.amza.shared;

import java.util.Arrays;

/**
 *
 */
public class TimestampedValue {

    private final long timestampId;
    private final byte[] value;

    public TimestampedValue(long timestampId, byte[] value) {
        this.timestampId = timestampId;
        this.value = value;
    }

    public long getTimestampId() {
        return timestampId;
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
        return Arrays.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        int result = (int) (timestampId ^ (timestampId >>> 32));
        result = 31 * result + (value != null ? Arrays.hashCode(value) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TimestampedValue{"
            + "timestampId=" + timestampId
            + ", value=" + Arrays.toString(value)
            + '}';
    }
}
