package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 * @author jonathan.colt
 */
public interface VersionedPartitionProvider {

    PartitionProperties getProperties(PartitionName partitionName) throws Exception;

    Iterable<PartitionName> getAllPartitions() throws Exception;

    Iterable<VersionedPartitionName> getMemberPartitions();
}
