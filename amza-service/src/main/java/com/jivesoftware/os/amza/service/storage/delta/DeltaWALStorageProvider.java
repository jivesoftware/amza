package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.wal.WALStorage;

/**
 *
 * @author jonathan.colt
 */
public interface DeltaWALStorageProvider {

    DeltaStripeWALStorage getDeltaWALStorage(PartitionName partitionName, WALStorage storage);
}
