package com.jivesoftware.os.amza.api;

import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionClientProvider {

    PartitionClient getPartition(PartitionName partitionName) throws Exception;
}
