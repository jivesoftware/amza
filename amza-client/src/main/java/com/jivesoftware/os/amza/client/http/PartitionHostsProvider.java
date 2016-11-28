package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import java.util.Optional;

/**
 * @author jonathan.colt
 */
public interface PartitionHostsProvider {

    RingPartitionProperties getRingPartitionProperties(PartitionName partitionName) throws Exception;

    void ensurePartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception;

    Ring getPartitionHosts(PartitionName partitionName, Optional<RingMemberAndHost> useHost, long waitForLeaderElection) throws Exception;

}
