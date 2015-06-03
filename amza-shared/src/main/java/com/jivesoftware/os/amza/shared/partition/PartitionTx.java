package com.jivesoftware.os.amza.shared.partition;

import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionTx<R> {

    R tx(VersionedPartitionName versionedPartitionName, Status partitionStatus) throws Exception;
}
