package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionHostsProvider {

    RingHost[] getPartitionHosts(PartitionName partitionName);
}
