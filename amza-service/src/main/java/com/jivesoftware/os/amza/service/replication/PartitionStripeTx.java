package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionStripeTx<R> {

    R tx(int deltaIndex, int stripeIndex, PartitionStripe partitionStripe) throws Exception;

}
