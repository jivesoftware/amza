package com.jivesoftware.os.amza.shared.partition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;

/**
 *
 * @author jonathan.colt
 */
@JsonInclude(Include.NON_NULL)
public class PartitionProperties {

    public WALStorageDescriptor walStorageDescriptor = new WALStorageDescriptor();
    public int replicationFactor = 0;
    public int takeFromFactor = 0;
    public boolean disabled = false;

    public PartitionProperties() {
    }

    public PartitionProperties(WALStorageDescriptor walStorageDescriptor,
        int replicationFactor,
        int takeFromFactor,
        boolean disabled) {
        this.walStorageDescriptor = walStorageDescriptor;
        this.replicationFactor = replicationFactor;
        this.takeFromFactor = takeFromFactor;
        this.disabled = disabled;
    }

}
