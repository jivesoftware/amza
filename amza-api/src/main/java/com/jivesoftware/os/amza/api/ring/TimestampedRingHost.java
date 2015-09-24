package com.jivesoftware.os.amza.api.ring;

/**
 *
 */
public class TimestampedRingHost {

    public final RingHost ringHost;
    public final long timestampId;

    public TimestampedRingHost(RingHost ringHost, long timestampId) {
        this.ringHost = ringHost;
        this.timestampId = timestampId;
    }
}
