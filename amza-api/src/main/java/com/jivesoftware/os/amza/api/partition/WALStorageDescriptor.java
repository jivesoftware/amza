package com.jivesoftware.os.amza.api.partition;

/**
 * @author jonathan.colt
 */
public class WALStorageDescriptor {

    public boolean ephemeral;
    public PrimaryIndexDescriptor primaryIndexDescriptor;
    public SecondaryIndexDescriptor[] secondaryIndexDescriptors;

    public int maxUpdatesBetweenCompactionHintMarker;
    public int maxUpdatesBetweenIndexCommitMarker;

    public WALStorageDescriptor() {
    }

    public WALStorageDescriptor(boolean ephemeral,
        PrimaryIndexDescriptor primaryIndexDescriptor,
        SecondaryIndexDescriptor[] secondaryIndexDescriptors,
        int maxUpdatesBetweenCompactionHintMarker,
        int maxUpdatesBetweenIndexCommitMarker) {
        this.ephemeral = ephemeral;
        this.primaryIndexDescriptor = primaryIndexDescriptor;
        this.secondaryIndexDescriptors = secondaryIndexDescriptors;
        this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
        this.maxUpdatesBetweenIndexCommitMarker = maxUpdatesBetweenIndexCommitMarker;
    }

}
