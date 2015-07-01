package com.jivesoftware.os.amza.shared.partition;

/**
 *
 * @author jonathan.colt
 */
public interface VersionedPartitionProvider {

    PartitionProperties getProperties(PartitionName partitionName);

    Iterable<VersionedPartitionName> getAllPartitions();
}
