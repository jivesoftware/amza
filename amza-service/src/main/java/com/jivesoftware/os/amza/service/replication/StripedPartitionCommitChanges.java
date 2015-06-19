package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;

/**
 *
 * @author jonathan.colt
 */
class StripedPartitionCommitChanges implements CommitChanges {
    final PartitionName partitionName;
    final PartitionStripeProvider partitionStripeProvider;
    private final boolean hardFlush;

    public StripedPartitionCommitChanges(PartitionName partitionName, PartitionStripeProvider partitionStripeProvider, boolean hardFlush) {
        this.partitionName = partitionName;
        this.partitionStripeProvider = partitionStripeProvider;
        this.hardFlush = hardFlush;
    }

    @Override
    public boolean shouldAwake(RingMember ringMember, long txId) throws Exception {
        return partitionStripeProvider.txPartition(partitionName,
            (com.jivesoftware.os.amza.service.replication.PartitionStripe stripe, com.jivesoftware.os.amza.shared.take.HighwaterStorage highwaterStorage) -> {
            return stripe.txPartition(partitionName,
                (VersionedPartitionName versionedPartitionName, TxPartitionStatus.Status partitionStatus) -> {
                Long highwater = highwaterStorage.get(ringMember, versionedPartitionName);
                return highwater == null || txId > highwater;
            });
        });
    }

    @Override
    public void commit(CommitTx commitTx) throws Exception {
        partitionStripeProvider.txPartition(partitionName,
            (com.jivesoftware.os.amza.service.replication.PartitionStripe stripe, com.jivesoftware.os.amza.shared.take.HighwaterStorage highwaterStorage) -> {
            stripe.txPartition(partitionName,
                (com.jivesoftware.os.amza.shared.partition.VersionedPartitionName versionedPartitionName, com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status partitionStatus) -> {
                if (partitionStatus == TxPartitionStatus.Status.KETCHUP || partitionStatus == TxPartitionStatus.Status.ONLINE) {
                    commitTx.tx(versionedPartitionName, highwaterStorage,
                        (com.jivesoftware.os.amza.shared.scan.Commitable<com.jivesoftware.os.amza.shared.wal.WALValue> commitable) -> {
                        return stripe.commit(highwaterStorage, partitionName, Optional.of(versionedPartitionName.getPartitionVersion()), false, commitable);
                    });
                }
                return false;
            });
            return null;
        });
        partitionStripeProvider.flush(partitionName, hardFlush);
    }

    @Override
    public String toString() {
        return "StripedPartitionCommitChanges{" + "partitionName=" + partitionName + ", partitionStripeProvider=" + partitionStripeProvider + '}';
    }

}
