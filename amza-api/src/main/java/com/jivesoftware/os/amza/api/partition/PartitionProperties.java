package com.jivesoftware.os.amza.api.partition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.stream.RowType;

/**
 * @author jonathan.colt
 */
@JsonInclude(Include.NON_NULL)
public class PartitionProperties {

    public WALStorageDescriptor walStorageDescriptor = new WALStorageDescriptor();
    public Consistency consistency;
    public boolean requireConsistency = true;
    public int takeFromFactor = 0;
    public boolean disabled = false;
    public RowType rowType = RowType.primary;

    public PartitionProperties() {
    }

    public PartitionProperties(WALStorageDescriptor walStorageDescriptor,
        Consistency consistency,
        boolean requireConsistency,
        int takeFromFactor,
        boolean disabled,
        RowType rowType) {
        this.walStorageDescriptor = walStorageDescriptor;
        this.consistency = consistency;
        this.requireConsistency = requireConsistency;
        this.takeFromFactor = takeFromFactor;
        this.disabled = disabled;
        this.rowType = rowType;
    }

}
