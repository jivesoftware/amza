package com.jivesoftware.os.amza.shared.partition;

import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;

/**
 *
 * @author jonathan.colt
 */
public interface HighestPartitionTx {

    void tx(VersionedPartitionName versionedPartitionName, Status partitionStatus, long highestTxId) throws Exception;
}
