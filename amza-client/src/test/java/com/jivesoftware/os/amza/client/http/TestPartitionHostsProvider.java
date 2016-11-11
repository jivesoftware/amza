package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import java.util.Optional;

/**
 *
 */
public class TestPartitionHostsProvider implements PartitionHostsProvider {

    private final int numHosts;

    public TestPartitionHostsProvider(int numHosts) {
        this.numHosts = numHosts;
    }

    @Override
    public PartitionProperties getPartitionProperties(PartitionName partitionName) throws Exception {
        return null;
    }

    @Override
    public void ensurePartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
        // do nothing
    }

    @Override
    public Ring getPartitionHosts(PartitionName partitionName, Optional<RingMemberAndHost> useHost, long waitForLeaderElection) throws Exception {
        RingMemberAndHost[] ringMemberAndHosts = new RingMemberAndHost[numHosts];
        for (int i = 0; i < numHosts; i++) {
            ringMemberAndHosts[i] = new RingMemberAndHost(new RingMember("test" + (i + 1)), new RingHost("", "", "host" + (i + 1), 1234));
        }
        return new Ring(0, ringMemberAndHosts);
    }
}
