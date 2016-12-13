package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionProvider {

    boolean createPartitionIfAbsent(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception;

    Partition getPartition(PartitionName partitionName) throws Exception;

    PartitionProperties getProperties(PartitionName partitionName) throws Exception;

    void updateProperties(PartitionName partitionName, PartitionProperties partitionProperties) throws Exception;

    RingMember awaitLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception;

    void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception;
}
