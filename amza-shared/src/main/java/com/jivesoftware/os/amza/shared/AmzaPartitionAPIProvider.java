package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.shared.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface AmzaPartitionAPIProvider {

    AmzaPartitionAPI getPartition(PartitionName partitionName) throws Exception;
}
