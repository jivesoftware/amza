package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jonathan.colt
 */
public class PartitionComposter {

    private final PartitionIndex partitionIndex;
    private final PartitionProvider partitionProvider;
    private final AmzaRingReader amzaRingReader;
    private final PartitionStripeProvider partitionStripeProvider;
    private final PartitionStatusStorage partitionStatusStorage;

    public PartitionComposter(PartitionIndex partitionIndex,
        PartitionProvider partitionProvider,
        AmzaRingReader amzaRingReader,
        PartitionStatusStorage partitionMemberStatusStorage,
        PartitionStripeProvider partitionStripeProvider) {

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
                PartitionStripe partitionStripe = partitionStripeProvider.getPartitionStripe(partitionName);
                VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
                if (partitionStripe.expungePartition(versionedPartitionName)) {
                    partitionIndex.remove(versionedPartitionName);
                    composted.add(versionedPartitionName);
                }
            } else if (!amzaRingReader.isMemberOfRing(partitionName.getRingName()) || !partitionProvider.hasPartition(partitionName)) {
                partitionStatusStorage.markForDisposal(new VersionedPartitionName(partitionName, versionedStatus.version), ringMember);
            }
            return true;
        });
        partitionStatusStorage.expunged(composted);

    }

}
