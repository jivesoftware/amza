package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionProvider {

    Partition getPartition(PartitionName partitionName) throws Exception;

    RingMember awaitLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception;

    void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception;
}
