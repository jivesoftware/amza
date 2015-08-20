package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionProvider {

    Partition getPartition(PartitionName partitionName) throws Exception;
}
