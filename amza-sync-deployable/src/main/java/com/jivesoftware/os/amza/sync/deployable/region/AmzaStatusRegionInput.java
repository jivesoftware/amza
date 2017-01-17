package com.jivesoftware.os.amza.sync.deployable.region;

import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 */
public class AmzaStatusRegionInput {
    public final String syncspaceName;
    public final PartitionName partitionName;

    public AmzaStatusRegionInput(String syncspaceName, PartitionName partitionName) {
        this.syncspaceName = syncspaceName;
        this.partitionName = partitionName;
    }
}
