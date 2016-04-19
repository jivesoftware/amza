package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.HighestPartitionTx;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;

/**
 *
 * @author jonathan.colt
 */
public interface TxHighestPartitionTx {

    long tx(PartitionStripe paritionStripe, VersionedAquarium versionedAquarium, HighestPartitionTx highestPartitionTx) throws Exception;
}
