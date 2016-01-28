package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;

/**
 *
 * @author jonathan.colt
 */
public interface TxHighestPartitionTx {

    long tx(VersionedAquarium versionedAquarium, HighestPartitionTx highestPartitionTx) throws Exception;
}
