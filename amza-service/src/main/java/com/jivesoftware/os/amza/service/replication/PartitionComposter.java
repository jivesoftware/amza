package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.HighwaterStorage;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PartitionComposter {

    private final AmzaStats amzaStats;
    private final PartitionIndex partitionIndex;
    private final PartitionProvider partitionProvider;
    private final AmzaRingStoreReader amzaRingReader;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStatusStorage partitionStatusStorage;

    public PartitionComposter(AmzaStats amzaStats,
        PartitionIndex partitionIndex,
        PartitionProvider partitionProvider,
        AmzaRingStoreReader amzaRingReader,
        PartitionStatusStorage partitionMemberStatusStorage,
        PartitionStripeProvider partitionStripeProvider) {

        this.amzaStats = amzaStats;
        this.partitionIndex = partitionIndex;
        this.partitionProvider = partitionProvider;
        this.amzaRingReader = amzaRingReader;
        this.partitionStatusStorage = partitionMemberStatusStorage;
        this.partitionStripeProvider = partitionStripeProvider;
    }

    public void compost() throws Exception {
        List<VersionedPartitionName> composted = new ArrayList<>();
        partitionStatusStorage.streamLocalState((partitionName, ringMember, versionedStatus) -> {
            if (versionedStatus.status == Status.EXPUNGE) {
                try {
                    amzaStats.beginCompaction("Expunge " + partitionName + " " + versionedStatus);
                    partitionStripeProvider.txPartition(partitionName, (PartitionStripe stripe, HighwaterStorage highwaterStorage) -> {
                        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
                        if (stripe.expungePartition(versionedPartitionName)) {
                            partitionIndex.remove(versionedPartitionName);
                            highwaterStorage.expunge(versionedPartitionName);
                            partitionProvider.destroyPartition(partitionName);
                            composted.add(versionedPartitionName);
                        }
                        return null;
                    });
                } finally {
                    amzaStats.endCompaction("Expunge " + partitionName + " " + versionedStatus);
                }

            } else if (!amzaRingReader.isMemberOfRing(partitionName.getRingName()) || !partitionProvider.hasPartition(partitionName)) {
                partitionStatusStorage.markForDisposal(new VersionedPartitionName(partitionName, versionedStatus.version), ringMember);
            }
            return true;
        });
        partitionStatusStorage.expunged(composted);

    }

}
