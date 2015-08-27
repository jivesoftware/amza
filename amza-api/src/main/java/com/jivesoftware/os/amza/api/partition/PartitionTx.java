package com.jivesoftware.os.amza.api.partition;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, PartitionState partitionState) throws Exception;
}
