package com.jivesoftware.os.amza.service.storage.delta;

import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.wal.WALStorage;

/**
 *
 * @author jonathan.colt
 */
public interface DeltaWALStorageProvider {

    StripeWALStorage getDeltaWALStorage(PartitionName partitionName, WALStorage storage);
}
