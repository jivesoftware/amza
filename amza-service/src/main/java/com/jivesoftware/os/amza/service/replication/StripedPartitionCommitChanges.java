package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALUpdated;

/**
 * @author jonathan.colt
 */
public class StripedPartitionCommitChanges implements CommitChanges {

    private final PartitionStripeProvider partitionStripeProvider;
    private final boolean hardFlush;
    private final WALUpdated walUpdated;

    public StripedPartitionCommitChanges(PartitionStripeProvider partitionStripeProvider,
        boolean hardFlush,
        WALUpdated walUpdated) {

        this.partitionStripeProvider = partitionStripeProvider;
        this.hardFlush = hardFlush;
        this.walUpdated = walUpdated;
    }

    @Override
    public void commit(VersionedPartitionName versionedPartitionName, CommitTx commitTx) throws Exception {
        PartitionName partitionName = versionedPartitionName.getPartitionName();
        partitionStripeProvider.txPartition(partitionName, (stripe, highwaterStorage) -> {
            return commitTx.tx(highwaterStorage, (prefix, commitable) -> stripe.commit(highwaterStorage,
                partitionName,
                Optional.of(versionedPartitionName.getPartitionVersion()),
                false,
                prefix,
                versionedAquarium -> -1,
                commitable,
                (versionedPartitionName1, leadershipToken, largestCommittedTxId) -> {
                },
                walUpdated));
        });
        partitionStripeProvider.flush(partitionName, hardFlush);
    }

    @Override
    public String toString() {
        return "StripedPartitionCommitChanges{" + "partitionStripeProvider=" + partitionStripeProvider + '}';
    }

}
