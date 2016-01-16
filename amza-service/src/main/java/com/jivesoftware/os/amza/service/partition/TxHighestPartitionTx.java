package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;

/**
 *
 * @author jonathan.colt
 */
public interface TxHighestPartitionTx<R> {

    R tx(VersionedAquarium versionedAquarium, HighestPartitionTx<R> highestPartitionTx) throws Exception;
}
