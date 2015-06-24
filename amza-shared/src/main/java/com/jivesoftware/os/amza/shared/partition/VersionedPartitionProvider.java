package com.jivesoftware.os.amza.shared.partition;

/**
 *
 * @author jonathan.colt
 */
public interface VersionedPartitionProvider {

    Iterable<VersionedPartitionName> getAllPartitions();
}
