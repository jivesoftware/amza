package com.jivesoftware.os.amza.api.partition;

/**
 *
 * @author jonathan.colt
 */
public interface HighestPartitionTx {

    void tx(VersionedPartitionName versionedPartitionName, PartitionState partitionState, long highestTxId) throws Exception;
}
