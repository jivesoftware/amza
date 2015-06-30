package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 *
 * @author jonathan.colt
 */
public class SystemPartitionCommitChanges implements CommitChanges {

    final SystemWALStorage systemWALStorage;
    final HighwaterStorage highwaterStorage;
    final WALUpdated walUpdated;

    public SystemPartitionCommitChanges(SystemWALStorage systemWALStorage,
        HighwaterStorage highwaterStorage,
        WALUpdated walUpdated) {
        this.systemWALStorage = systemWALStorage;
        this.highwaterStorage = highwaterStorage;
        this.walUpdated = walUpdated;
    }

    @Override
    public void commit(VersionedPartitionName versionedPartitionName, CommitTx commitTx) throws Exception {
        commitTx.tx(highwaterStorage, commitable -> systemWALStorage.update(versionedPartitionName, commitable, walUpdated));
        highwaterStorage.flush(null);
    }

    @Override
    public String toString() {
        return "SystemPartitionCommitChanges{" + "systemWALStorage=" + systemWALStorage + '}';
    }

}
