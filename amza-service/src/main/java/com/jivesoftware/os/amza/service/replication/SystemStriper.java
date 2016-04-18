package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface SystemStriper {

    // Sucks but its our legacy
    int getSystemStripe(PartitionName partitionName);

}
