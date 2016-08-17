package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.RingMembership;

/**
 * @author jonathan.colt
 */
public interface VersionedPartitionProvider {

    boolean hasPartition(PartitionName partitionName) throws Exception;

    PartitionProperties getProperties(PartitionName partitionName) throws Exception;

    VersionedPartitionProperties getVersionedProperties(PartitionName partitionName,
        VersionedPartitionProperties versionedPartitionProperties);

    long getPartitionDisposal(PartitionName partitionName) throws Exception;

    class VersionedPartitionProperties {

        public final long version;
        public final PartitionProperties properties;

        public VersionedPartitionProperties(long version, PartitionProperties properties) {
            this.version = version;
            this.properties = properties;
        }

    }

    Iterable<PartitionName> getMemberPartitions(RingMembership ringMembership) throws Exception;
}
