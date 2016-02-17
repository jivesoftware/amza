package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.RingMembership;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;

/**
 * @author jonathan.colt
 */
public interface VersionedPartitionProvider {

    PartitionProperties getProperties(PartitionName partitionName) throws Exception;

    PartitionIndex.VersionedPartitionProperties getVersionedProperties(PartitionName partitionName,
        VersionedPartitionProperties versionedPartitionProperties);

    class VersionedPartitionProperties {

        public final long vesion;
        public final PartitionProperties properties;

        public VersionedPartitionProperties(long vesion, PartitionProperties properties) {
            this.vesion = vesion;
            this.properties = properties;
        }

    }

    Iterable<PartitionName> getMemberPartitions(RingMembership ringMembership) throws Exception;
}
