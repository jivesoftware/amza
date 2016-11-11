package com.jivesoftware.os.amza.api;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionClientProvider {

    PartitionClient getPartition(PartitionName partitionName) throws Exception;

    PartitionClient getPartition(PartitionName partitionName,
        int ringSize,
        PartitionProperties partitionProperties) throws Exception;

    PartitionProperties getProperties(PartitionName partitionName) throws Exception;
}
