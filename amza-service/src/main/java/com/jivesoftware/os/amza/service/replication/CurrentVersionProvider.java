package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 *
 */
public interface CurrentVersionProvider {

    boolean isCurrentVersion(VersionedPartitionName versionedPartitionName);

    void abandonVersion(VersionedPartitionName versionedPartitionName) throws Exception;
}
