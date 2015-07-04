package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.shared.partition.PartitionProperties;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionPropertyMarshaller {

    PartitionProperties fromBytes(byte[] bytes);

    byte[] toBytes(PartitionProperties partitionProperties);
}
