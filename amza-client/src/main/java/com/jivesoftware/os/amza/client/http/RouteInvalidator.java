package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.partition.PartitionName;

/**
 *
 * @author jonathan.colt
 */
public interface RouteInvalidator {

    void invalidateRouting(PartitionName partitionName);
}
