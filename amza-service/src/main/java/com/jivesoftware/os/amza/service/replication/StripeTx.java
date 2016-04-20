package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
public interface StripeTx<R> {

    R tx(TxPartitionStripe txPartitionStripe, HighwaterStorage highwaterStorage, VersionedAquarium versionedAquarium) throws Exception;

    interface TxPartitionStripe {

        <S> S tx(PartitionStripeTx<S> partitionStripeTx) throws Exception;
    }
}
