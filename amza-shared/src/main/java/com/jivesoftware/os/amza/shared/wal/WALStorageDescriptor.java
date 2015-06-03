package com.jivesoftware.os.amza.shared.wal;

import com.jivesoftware.os.amza.shared.region.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.region.SecondaryIndexDescriptor;

/**
 *
 * @author jonathan.colt
 */
public class WALStorageDescriptor {

    public PrimaryIndexDescriptor primaryIndexDescriptor;
    public SecondaryIndexDescriptor[] secondaryIndexDescriptors;

    public int maxUpdatesBetweenCompactionHintMarker;
    public int maxUpdatesBetweenIndexCommitMarker;

    public WALStorageDescriptor() {
    }

    public WALStorageDescriptor(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors,
        int maxUpdatesBetweenCompactionHintMarker,
        int maxUpdatesBetweenIndexCommitMarker
    ) {
        this.primaryIndexDescriptor = primaryIndexDescriptor;
        this.secondaryIndexDescriptors = secondaryIndexDescriptors;
        this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
        this.maxUpdatesBetweenIndexCommitMarker = maxUpdatesBetweenIndexCommitMarker;
    }

}
