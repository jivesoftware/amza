package com.jivesoftware.os.amza.api.partition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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
    public boolean replicated = true;
    public boolean disabled = false;
    public RowType rowType = RowType.primary;

    public String indexClassName;
    public int maxValueSizeInIndex = -1;
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
        boolean replicated,
        boolean disabled,
        RowType rowType,
        String indexClassName,
        int maxValueSizeInIndex,
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

        Preconditions.checkArgument(replicated || consistency == Consistency.none, "Consistency:%s requires replication", consistency);
        this.consistency = consistency;
        this.requireConsistency = requireConsistency;
        this.replicated = replicated;

        this.disabled = disabled;
        this.rowType = rowType;
        this.indexClassName = indexClassName;
        this.maxValueSizeInIndex = maxValueSizeInIndex;
        this.indexProperties = indexProperties;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.maxLeaps = maxLeaps;
    }

    @JsonIgnore
    public PartitionProperties copy() {
        return new PartitionProperties(durability, tombstoneTimestampAgeInMillis, tombstoneTimestampIntervalMillis, tombstoneVersionAgeInMillis,
            tombstoneVersionIntervalMillis, ttlTimestampAgeInMillis, ttlTimestampIntervalMillis, ttlVersionAgeInMillis, ttlVersionIntervalMillis,
            forceCompactionOnStartup, consistency, requireConsistency, replicated, disabled, rowType, indexClassName, maxValueSizeInIndex,
            indexProperties == null ? null : Maps.newHashMap(indexProperties),
            updatesBetweenLeaps,
            maxLeaps);
    }

    @Override
    public String toString() {
        return "PartitionProperties{"
            + "durability=" + durability
            + ", tombstoneTimestampAgeInMillis=" + tombstoneTimestampAgeInMillis
            + ", tombstoneTimestampIntervalMillis=" + tombstoneTimestampIntervalMillis
            + ", tombstoneVersionAgeInMillis=" + tombstoneVersionAgeInMillis
            + ", tombstoneVersionIntervalMillis=" + tombstoneVersionIntervalMillis
            + ", ttlTimestampAgeInMillis=" + ttlTimestampAgeInMillis
            + ", ttlTimestampIntervalMillis=" + ttlTimestampIntervalMillis
            + ", ttlVersionAgeInMillis=" + ttlVersionAgeInMillis
            + ", ttlVersionIntervalMillis=" + ttlVersionIntervalMillis
            + ", forceCompactionOnStartup=" + forceCompactionOnStartup
            + ", consistency=" + consistency
            + ", requireConsistency=" + requireConsistency
            + ", replicated=" + replicated
            + ", disabled=" + disabled
            + ", rowType=" + rowType
            + ", indexClassName='" + indexClassName + '\''
            + ", maxValueSizeInIndex=" + maxValueSizeInIndex
            + ", indexProperties=" + indexProperties
            + ", updatesBetweenLeaps=" + updatesBetweenLeaps
            + ", maxLeaps=" + maxLeaps
            + '}';
    }
}
