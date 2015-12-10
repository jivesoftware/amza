package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;

/**
 * @author jonathan.colt
 */
public interface WALUpdated {

    void updated(VersionedPartitionName versionedPartitionName, long txId) throws Exception;

}
