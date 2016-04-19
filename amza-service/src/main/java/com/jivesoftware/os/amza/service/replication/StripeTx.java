package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
public interface StripeTx<R> {

    R tx(int stripe, PartitionStripe partitionStripe, HighwaterStorage highwaterStorage, VersionedAquarium versionedAquarium) throws Exception;

}
