package com.jivesoftware.os.amza.service.storage.replication;

import com.jivesoftware.os.amza.shared.RingHost;

class RingWalker {
    private final RingHost[] ringHosts;
    private final int replicationFactor;
    int replicated = 0;
    int loops = 0;
    int failed = 0;

    public RingWalker(RingHost[] ringHosts, int replicationFactor) {
        this.ringHosts = ringHosts;
        this.replicationFactor = replicationFactor;
    }

    public RingHost host() {
        int i = failed + (loops * 2);
        if (i >= ringHosts.length) {
            return null;
        }
        if (replicated >= replicationFactor) {
            return null;
        }
        RingHost ringHost = ringHosts[i];
        ringHosts[i] = null;
        return ringHost;
    }

    public void success() {
        loops++;
        failed = 0;
        replicated++;
    }

    public void failed() {
        failed++;
    }

    public boolean wasAdequetlyReplicated() {
        return replicated >= replicationFactor;
    }

}
