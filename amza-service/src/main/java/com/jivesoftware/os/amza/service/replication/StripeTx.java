package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 *
 * @author jonathan.colt
 */
public interface StripeTx<R> {

    R tx(PartitionStripePromise partitionStripePromise, HighwaterStorage highwaterStorage, VersionedAquarium versionedAquarium) throws Exception;

    interface PartitionStripePromise {

        <S> S get(PartitionStripeTx<S> partitionStripeTx) throws Exception;
    }
}
