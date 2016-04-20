package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.replication.StripeTx.PartitionStripePromise;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;

/**
 * @author jonathan.colt
 */
public class SystemPartitionCommitChanges implements CommitChanges {

    final StorageVersionProvider storageVersionProvider;
    final SystemWALStorage systemWALStorage;
    final HighwaterStorage highwaterStorage;
    final WALUpdated walUpdated;

    public SystemPartitionCommitChanges(
        StorageVersionProvider storageVersionProvider,
        SystemWALStorage systemWALStorage,
        HighwaterStorage highwaterStorage,
        WALUpdated walUpdated) {

        this.storageVersionProvider = storageVersionProvider;
        this.systemWALStorage = systemWALStorage;
        this.highwaterStorage = highwaterStorage;
        this.walUpdated = walUpdated;
    }

    @Override
    public void commit(VersionedPartitionName versionedPartitionName, CommitTx commitTx) throws Exception {
        commitTx.tx(highwaterStorage,
            new VersionedAquarium(versionedPartitionName, null, 0),
            (prefix, commitable) -> systemWALStorage.update(versionedPartitionName, prefix, commitable, walUpdated));
        highwaterStorage.flush(null);
    }

    @Override
    public String toString() {
        return "SystemPartitionCommitChanges{" + "systemWALStorage=" + systemWALStorage + '}';
    }

}
