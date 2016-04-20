package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import java.util.concurrent.Callable;

/**
 *
 */
public interface CurrentVersionProvider {

    interface StripeIndexs<R> {

        R tx(int deltaIndex, int stripeIndex, StorageVersion storageVersion) throws Exception;
    }

    <R> R tx(PartitionName partitionName, boolean createIfAbsent, StripeIndexs<R> tx) throws Exception;

    void invalidateDeltaIndexCache(VersionedPartitionName versionedPartitionName, Callable<Boolean> invalidatable) throws Exception;

    boolean isCurrentVersion(VersionedPartitionName versionedPartitionName);

    void abandonVersion(VersionedPartitionName versionedPartitionName) throws Exception;

}
