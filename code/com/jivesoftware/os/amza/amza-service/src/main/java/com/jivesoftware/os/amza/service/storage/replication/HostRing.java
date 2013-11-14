package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.shared.RingHost;

public class HostRing {

    private final RingHost[] aboveRing;
    private final RingHost[] belowRing;

    public HostRing(RingHost[] aboveRing, RingHost[] belowRing) {
        this.aboveRing = aboveRing;
        this.belowRing = belowRing;
    }

    public RingHost[] getAboveRing() {
        return aboveRing;
    }

    public RingHost[] getBelowRing() {
        return belowRing;
    }
}