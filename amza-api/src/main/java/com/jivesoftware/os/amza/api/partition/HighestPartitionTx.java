package com.jivesoftware.os.amza.api.partition;

import com.jivesoftware.os.amza.api.partition.TxPartitionStatus.Status;

/**
 *
 * @author jonathan.colt
 */
public interface HighestPartitionTx {

    void tx(VersionedPartitionName versionedPartitionName, Status partitionStatus, long highestTxId) throws Exception;
}
