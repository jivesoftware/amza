package com.jivesoftware.os.amza.service.storage;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import java.io.IOException;

/**
 *
 * @author jonathan.colt
 */
public interface PartitionPropertyMarshaller {

    PartitionProperties fromBytes(byte[] bytes) throws IOException;

    byte[] toBytes(PartitionProperties partitionProperties) throws IOException;
}
