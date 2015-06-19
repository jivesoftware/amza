package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Commitable;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import com.jivesoftware.os.amza.shared.wal.WALValue;

/**
 *
 * @author jonathan.colt
 */
class SystemPartitionCommitChanges implements CommitChanges {

    final VersionedPartitionName versionedPartitionName;
    final SystemWALStorage systemWALStorage;
    final HighwaterStorage highwaterStorage;

    public SystemPartitionCommitChanges(VersionedPartitionName versionedPartitionName, SystemWALStorage systemWALStorage, HighwaterStorage highwaterStorage) {
        this.versionedPartitionName = versionedPartitionName;
        this.systemWALStorage = systemWALStorage;
        this.highwaterStorage = highwaterStorage;
    }

    @Override
    public boolean shouldAwake(RingMember ringMember, long txId) throws Exception {
        highwaterStorage.get(ringMember, versionedPartitionName);
        Long highwater = highwaterStorage.get(ringMember, versionedPartitionName);
        return highwater == null || txId > highwater;
    }

    @Override
    public void commit(CommitTx commitTx) throws Exception {
        commitTx.tx(versionedPartitionName, highwaterStorage,
            (Commitable<WALValue> commitable) -> {
                return systemWALStorage.update(versionedPartitionName, commitable);
            });
        highwaterStorage.flush(null);
    }

    @Override
    public String toString() {
        return "SystemPartitionCommitChanges{" + "versionedPartitionName=" + versionedPartitionName + ", systemWALStorage=" + systemWALStorage + '}';
    }

}
