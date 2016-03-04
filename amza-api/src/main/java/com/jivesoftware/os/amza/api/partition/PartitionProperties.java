package com.jivesoftware.os.amza.api.partition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.stream.RowType;
import java.util.Map;

/**
 * @author jonathan.colt
 */
@JsonInclude(Include.NON_NULL)
public class PartitionProperties {

    public Durability durability;

    public long tombstoneTimestampAgeInMillis = 0;
    public long tombstoneTimestampIntervalMillis = 0;
    public long tombstoneVersionAgeInMillis = 0;
    public long tombstoneVersionIntervalMillis = 0;
    public long ttlTimestampAgeInMillis = 0;
    public long ttlTimestampIntervalMillis = 0;
    public long ttlVersionAgeInMillis = 0;
    public long ttlVersionIntervalMillis = 0;

    public boolean forceCompactionOnStartup = false;

    public Consistency consistency;
    public boolean requireConsistency = true;
    public int takeFromFactor = 0;
    public boolean disabled = false;
    public RowType rowType = RowType.primary;

    public String indexClassName;
    public Map<String, String> indexProperties;
    public int updatesBetweenLeaps = -1;
    public int maxLeaps = -1;

    public PartitionProperties() {
    }

    public PartitionProperties(Durability durability,
        long tombstoneTimestampAgeInMillis,
        long tombstoneTimestampIntervalMillis,
        long tombstoneVersionAgeInMillis,
        long tombstoneVersionIntervalMillis,
        long ttlTimestampAgeInMillis,
        long ttlTimestampIntervalMillis,
        long ttlVersionAgeInMillis,
        long ttlVersionIntervalMillis,
        boolean forceCompactionOnStartup,
        Consistency consistency,
        boolean requireConsistency,
        int takeFromFactor,
        boolean disabled,
        RowType rowType,
        String indexClassName,
        Map<String, String> indexProperties,
        int updatesBetweenLeaps,
        int maxLeaps) {
        this.durability = durability;
        this.tombstoneTimestampAgeInMillis = tombstoneTimestampAgeInMillis;
        this.tombstoneTimestampIntervalMillis = tombstoneTimestampIntervalMillis;
        this.tombstoneVersionAgeInMillis = tombstoneVersionAgeInMillis;
        this.tombstoneVersionIntervalMillis = tombstoneVersionIntervalMillis;
        this.ttlTimestampAgeInMillis = ttlTimestampAgeInMillis;
        this.ttlTimestampIntervalMillis = ttlTimestampIntervalMillis;
        this.ttlVersionAgeInMillis = ttlVersionAgeInMillis;
        this.ttlVersionIntervalMillis = ttlVersionIntervalMillis;
        this.forceCompactionOnStartup = forceCompactionOnStartup;

        Preconditions.checkArgument(takeFromFactor > 0 || consistency == Consistency.none, "Consistency:%s requires a takeFromFactor > 0", consistency);
        this.consistency = consistency;
        this.requireConsistency = requireConsistency;
        this.takeFromFactor = takeFromFactor;

        this.disabled = disabled;
        this.rowType = rowType;
        this.indexClassName = indexClassName;
        this.indexProperties = indexProperties;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.maxLeaps = maxLeaps;
    }

    @Override
    public String toString() {
        return "PartitionProperties{" +
            "durability=" + durability +
            ", tombstoneTimestampAgeInMillis=" + tombstoneTimestampAgeInMillis +
            ", tombstoneTimestampIntervalMillis=" + tombstoneTimestampIntervalMillis +
            ", tombstoneVersionAgeInMillis=" + tombstoneVersionAgeInMillis +
            ", tombstoneVersionIntervalMillis=" + tombstoneVersionIntervalMillis +
            ", ttlTimestampAgeInMillis=" + ttlTimestampAgeInMillis +
            ", ttlTimestampIntervalMillis=" + ttlTimestampIntervalMillis +
            ", ttlVersionAgeInMillis=" + ttlVersionAgeInMillis +
            ", ttlVersionIntervalMillis=" + ttlVersionIntervalMillis +
            ", forceCompactionOnStartup=" + forceCompactionOnStartup +
            ", consistency=" + consistency +
            ", requireConsistency=" + requireConsistency +
            ", takeFromFactor=" + takeFromFactor +
            ", disabled=" + disabled +
            ", rowType=" + rowType +
            ", indexClassName='" + indexClassName + '\'' +
            ", indexProperties=" + indexProperties +
            ", updatesBetweenLeaps=" + updatesBetweenLeaps +
            ", maxLeaps=" + maxLeaps +
            '}';
    }
}
