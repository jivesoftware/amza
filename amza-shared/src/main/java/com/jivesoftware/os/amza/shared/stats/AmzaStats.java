package com.jivesoftware.os.amza.shared.stats;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class AmzaStats {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
    private static final JiveEpochTimestampProvider jiveEpochTimestampProvider = new JiveEpochTimestampProvider();

    private final Map<RingMember, AtomicLong> took = new ConcurrentSkipListMap<>();
    private final Map<RingMember, AtomicLong> offered = new ConcurrentHashMap<>();

    private final Map<String, Long> ongoingCompaction = new ConcurrentHashMap<>();
    private final List<Entry<String, Long>> recentCompaction = new ArrayList<>();
    private final AtomicLong totalCompactions = new AtomicLong();

    private final Totals grandTotals = new Totals();
    private final Map<PartitionName, Totals> partitionTotals = new ConcurrentHashMap<>();

    public final Multiset<RingMember> takeErrors = ConcurrentHashMultiset.create();
    public final Multiset<RingMember> replicateErrors = ConcurrentHashMultiset.create();

    public final IoStats ioStats = new IoStats();
    public final NetStats netStats = new NetStats();

    public AmzaStats() {
    }

    static public class Totals {

        public final AtomicLong gets = new AtomicLong();
        public final AtomicLong getsLag = new AtomicLong();
        public final AtomicLong scans = new AtomicLong();
        public final AtomicLong scansLag = new AtomicLong();
        public final AtomicLong takes = new AtomicLong();
        public final AtomicLong takesLag = new AtomicLong();
        public final AtomicLong takeApplies = new AtomicLong();
        public final AtomicLong takeAppliesLag = new AtomicLong();
        public final AtomicLong replicates = new AtomicLong();
        public final AtomicLong replicatesLag = new AtomicLong();
        public final AtomicLong received = new AtomicLong();
        public final AtomicLong receivedLag = new AtomicLong();
        public final AtomicLong receivedApplies = new AtomicLong();
        public final AtomicLong receivedAppliesLag = new AtomicLong();
        public final AtomicLong directApplies = new AtomicLong();
        public final AtomicLong directAppliesLag = new AtomicLong();
    }

    public void took(RingMember member) {
        AtomicLong got = took.get(member);
        if (got == null) {
            got = new AtomicLong();
            took.put(member, got);
        }
        got.incrementAndGet();
    }

    public long getTotalTakes(RingMember member) {
        AtomicLong got = took.get(member);
        if (got == null) {
            return 0;
        }
        return got.get();
    }

    public void offered(Entry<RingMember, RingHost> node) {
        AtomicLong got = offered.get(node.getKey());
        if (got == null) {
            got = new AtomicLong();
            offered.put(node.getKey(), got);
        }
        got.incrementAndGet();
    }

    public long getTotalOffered(RingMember member) {
        AtomicLong got = offered.get(member);
        if (got == null) {
            return 0;
        }
        return got.get();
    }

    public void beginCompaction(String name) {
        ongoingCompaction.put(name, System.currentTimeMillis());
    }

    public void endCompaction(String name) {
        Long start = ongoingCompaction.remove(name);
        totalCompactions.incrementAndGet();
        if (start != null) {
            recentCompaction.add(new AbstractMap.SimpleEntry<>(name, System.currentTimeMillis() - start));
            while (recentCompaction.size() > 30) {
                recentCompaction.remove(0);
            }
        }
    }

    public List<Entry<String, Long>> recentCompaction() {
        return recentCompaction;
    }

    public List<Entry<String, Long>> ongoingCompactions() {
        List<Entry<String, Long>> ongoing = new ArrayList<>();
        for (Entry<String, Long> e : ongoingCompaction.entrySet()) {
            ongoing.add(new AbstractMap.SimpleEntry<>(e.getKey(), System.currentTimeMillis() - e.getValue()));
        }
        return ongoing;
    }

    public long getTotalCompactions() {
        return totalCompactions.get();
    }

    public Totals getGrandTotal() {
        return grandTotals;
    }

    public void took(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.takes.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.takes.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.takesLag.set(lag);
        grandTotals.takesLag.set((grandTotals.takesLag.get() + lag) / 2);
    }

    public void tookApplied(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.takeApplies.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.takeApplies.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.takeAppliesLag.set(lag);
        grandTotals.takesLag.set((grandTotals.takesLag.get() + lag) / 2);
    }

    public void direct(PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.directApplies.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.directApplies.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.directAppliesLag.set(lag);
        grandTotals.directAppliesLag.set((grandTotals.directAppliesLag.get() + lag) / 2);
    }

    public void replicated(RingMember to, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.replicates.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.replicates.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.replicatesLag.set(lag);
        grandTotals.replicatesLag.set((grandTotals.replicatesLag.get() + lag) / 2);
    }

    public void received(PartitionName versionedPartitionName, int count, long smallestTxId) {
        grandTotals.received.addAndGet(count);
        Totals totals = partitionTotals(versionedPartitionName);
        totals.received.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.receivedLag.set(lag);
        grandTotals.receivedLag.set((grandTotals.receivedLag.get() + lag) / 2);
    }

    public void receivedApplied(PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.receivedApplies.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.receivedApplies.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.receivedAppliesLag.set(lag);
        grandTotals.receivedAppliesLag.set((grandTotals.receivedAppliesLag.get() + lag) / 2);
    }

    private Totals partitionTotals(PartitionName versionedPartitionName) {
        Totals got = partitionTotals.get(versionedPartitionName);
        if (got == null) {
            got = new Totals();
            partitionTotals.put(versionedPartitionName, got);
        }
        return got;
    }

    public Map<PartitionName, Totals> getPartitionTotals() {
        return partitionTotals;
    }

    private void logChanges(String name, RowsChanged changed) {
        if (!changed.isEmpty()) {
            VersionedPartitionName versionedPartitionName = changed.getVersionedPartitionName();
            PartitionName partitionName = versionedPartitionName.getPartitionName();

            LOG.debug("{} {} to partition: {}:{} lag:{}", new Object[]{name,
                changed.getApply().size(),
                partitionName.getPartitionName(),
                partitionName.getRingName(),
                lag(changed)});
        }
    }

    long lag(RowsChanged changed) {
        return lag(changed.getOldestRowTxId());
    }

    long lag(long oldest) {
        if (oldest != Long.MAX_VALUE) {
            long[] unpack = snowflakeIdPacker.unpack(oldest);
            long lag = jiveEpochTimestampProvider.getApproximateTimestamp(System.currentTimeMillis()) - unpack[0];
            return lag;
        }
        return 0;
    }

}
