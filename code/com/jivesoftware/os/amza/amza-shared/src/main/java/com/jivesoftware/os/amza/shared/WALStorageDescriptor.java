package com.jivesoftware.os.amza.shared;

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
