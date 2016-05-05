package com.jivesoftware.os.amza.service.stats;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class AmzaStats {

    private static final SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
    private static final JiveEpochTimestampProvider jiveEpochTimestampProvider = new JiveEpochTimestampProvider();

    private final Map<RingMember, AtomicLong> took = new ConcurrentSkipListMap<>();

    private final Map<CompactionFamily, Map<String, Long>> ongoingCompaction = new ConcurrentHashMap<>();
    private final Map<CompactionFamily, AtomicLong> totalCompactions = new ConcurrentHashMap<>();
    private final List<Entry<String, Long>> recentCompaction = new ArrayList<>();

    public final Map<RingMember, AtomicLong> longPolled = new ConcurrentSkipListMap<>();
    public final Map<RingMember, AtomicLong> longPollAvailables = new ConcurrentSkipListMap<>();

    private final Totals grandTotals = new Totals();
    private final Map<PartitionName, Totals> partitionTotals = new ConcurrentHashMap<>();

    public final Multiset<RingMember> takeErrors = ConcurrentHashMultiset.create();

    public final IoStats ioStats = new IoStats();
    public final NetStats netStats = new NetStats();

    public final AtomicLong addMember = new AtomicLong();
    public final AtomicLong removeMember = new AtomicLong();
    public final AtomicLong getRing = new AtomicLong();
    public final AtomicLong rowsStream = new AtomicLong();
    public final AtomicLong completedRowsStream = new AtomicLong();

    public final AtomicLong availableRowsStream = new AtomicLong();
    public final AtomicLong rowsTaken = new AtomicLong();
    public final AtomicLong completedRowsTake = new AtomicLong();

    public final AtomicLong backPressure = new AtomicLong();
    public final AtomicLong pushBacks = new AtomicLong();

    public long[] deltaStripeMergeLoaded = new long[0];
    public double[] deltaStripeLoad = new double[0];

    public long[] deltaStripeMergePending = new long[0];
    public double[] deltaStripeMerge = new double[0];

    public final AtomicLong deltaFirstCheckRemoves = new AtomicLong();
    public final AtomicLong deltaSecondCheckRemoves = new AtomicLong();

    public final AtomicLong takes = new AtomicLong();
    public final AtomicLong takeExcessRows = new AtomicLong();

    public AmzaStats() {
    }

    public void deltaStripeLoad(int index, long count, double load) {
        long[] copyCount = deltaStripeMergeLoaded;
        double[] copy = deltaStripeLoad;
        if (index >= copy.length) {
            double[] newArray = new double[index + 1];
            System.arraycopy(copy, 0, newArray, 0, copy.length);
            copy = newArray;
            deltaStripeLoad = copy;
        }
        if (index >= copyCount.length) {
            long[] newArrayCount = new long[index + 1];
            System.arraycopy(copyCount, 0, newArrayCount, 0, copyCount.length);
            copyCount = newArrayCount;
            deltaStripeMergeLoaded = copyCount;
        }
        copyCount[index] = count;
        copy[index] = load;

    }

    public void deltaStripeMerge(int index, long count, double load) {
        long[] copyCount = deltaStripeMergePending;
        double[] copy = deltaStripeMerge;
        if (index >= copy.length) {
            long[] newArrayCount = new long[index + 1];
            double[] newArray = new double[index + 1];
            System.arraycopy(copyCount, 0, newArrayCount, 0, copyCount.length);
            System.arraycopy(copy, 0, newArray, 0, copy.length);
            copyCount = newArrayCount;
            copy = newArray;
            deltaStripeMergePending = copyCount;
            deltaStripeMerge = copy;
        }
        copyCount[index] = count;
        copy[index] = load;
    }

    static public class Totals {

        public final AtomicLong gets = new AtomicLong();
        public final AtomicLong getsLatency = new AtomicLong();
        public final AtomicLong scans = new AtomicLong();
        public final AtomicLong scansLatency = new AtomicLong();
        public final AtomicLong updates = new AtomicLong();
        public final AtomicLong updatesLag = new AtomicLong();
        public final AtomicLong offers = new AtomicLong();
        public final AtomicLong offersLag = new AtomicLong();
        public final AtomicLong takes = new AtomicLong();
        public final AtomicLong takesLag = new AtomicLong();
        public final AtomicLong takeApplies = new AtomicLong();
        public final AtomicLong takeAppliesLag = new AtomicLong();
        public final AtomicLong directApplies = new AtomicLong();
        public final AtomicLong directAppliesLag = new AtomicLong();
        public final AtomicLong acks = new AtomicLong();
        public final AtomicLong acksLag = new AtomicLong();
        public final AtomicLong quorums = new AtomicLong();
        public final AtomicLong quorumsLatency = new AtomicLong();
        public final AtomicLong quorumTimeouts = new AtomicLong();
    }

    public void longPolled(RingMember member) {
        longPolled.computeIfAbsent(member, (key) -> new AtomicLong()).incrementAndGet();
    }

    public void longPollAvailables(RingMember member) {
        longPollAvailables.computeIfAbsent(member, (key) -> new AtomicLong()).incrementAndGet();
    }

    public void took(RingMember member) {
        took.computeIfAbsent(member, (key) -> new AtomicLong()).incrementAndGet();
    }

    public long getTotalTakes(RingMember member) {
        AtomicLong got = took.get(member);
        if (got == null) {
            return 0;
        }
        return got.get();
    }

    public static enum CompactionFamily {
        expunge, tombstone, merge;
    }

    public int ongoingCompaction(CompactionFamily family) {
        Map<String, Long> got = ongoingCompaction.computeIfAbsent(family, (key) -> new ConcurrentHashMap<>());
        return got.size();
    }

    public void beginCompaction(CompactionFamily family, String name) {
        Map<String, Long> got = ongoingCompaction.computeIfAbsent(family, (key) -> new ConcurrentHashMap<>());
        got.put(name, System.currentTimeMillis());
    }

    public void endCompaction(CompactionFamily family, String name) {
        Map<String, Long> got = ongoingCompaction.computeIfAbsent(family, (key) -> new ConcurrentHashMap<>());
        Long start = got.remove(name);

        totalCompactions.computeIfAbsent(family, (key) -> new AtomicLong()).incrementAndGet();
        if (start != null) {
            recentCompaction.add(new AbstractMap.SimpleEntry<>(family + " " + name, System.currentTimeMillis() - start));
            while (recentCompaction.size() > 10_000) {
                recentCompaction.remove(0);
            }
        }
    }

    public List<Entry<String, Long>> recentCompaction() {
        return recentCompaction;
    }

    public List<Entry<String, Long>> ongoingCompactions(CompactionFamily... families) {
        List<Entry<String, Long>> ongoing = new ArrayList<>();
        for (CompactionFamily family : families) {
            Map<String, Long> got = ongoingCompaction.get(family);
            if (got != null) {
                for (Entry<String, Long> e : got.entrySet()) {
                    ongoing.add(new AbstractMap.SimpleEntry<>(e.getKey(), System.currentTimeMillis() - e.getValue()));
                }
            }
        }
        return ongoing;
    }

    public long getTotalCompactions(CompactionFamily family) {
        return totalCompactions.computeIfAbsent(family, (key) -> new AtomicLong()).get();
    }

    public Totals getGrandTotal() {
        return grandTotals;
    }

    public void updates(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.updates.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.updates.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.updatesLag.set(lag);
        grandTotals.updatesLag.set((grandTotals.updatesLag.get() + lag) / 2);
    }

    public void offers(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.offers.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.offers.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.offersLag.set(lag);
        grandTotals.offersLag.set((grandTotals.offersLag.get() + lag) / 2);
    }

    public void acks(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.acks.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.acks.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.acksLag.set(lag);
        grandTotals.acksLag.set((grandTotals.acksLag.get() + lag) / 2);
    }

    public void quorums(PartitionName partitionName, int count, long lag) {
        grandTotals.quorums.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.quorums.addAndGet(count);
        totals.quorumsLatency.set(lag);
        grandTotals.quorumsLatency.set((grandTotals.quorumsLatency.get() + lag) / 2);
    }

    public void quorumTimeouts(PartitionName partitionName, int count) {
        grandTotals.quorumTimeouts.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.quorumTimeouts.addAndGet(count);
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
        grandTotals.takeAppliesLag.set((grandTotals.takeAppliesLag.get() + lag) / 2);
    }

    public void direct(PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.directApplies.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.directApplies.addAndGet(count);
        long lag = lag(smallestTxId);
        totals.directAppliesLag.set(lag);
        grandTotals.directAppliesLag.set((grandTotals.directAppliesLag.get() + lag) / 2);
    }

    public void gets(PartitionName partitionName, int count, long lag) {
        grandTotals.gets.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.gets.addAndGet(count);
        totals.getsLatency.set(lag);
        grandTotals.getsLatency.set((grandTotals.getsLatency.get() + lag) / 2);
    }

    public void scans(PartitionName partitionName, int count, long lag) {
        grandTotals.scans.addAndGet(count);
        Totals totals = partitionTotals(partitionName);
        totals.scans.addAndGet(count);
        totals.scansLatency.set(lag);
        grandTotals.scansLatency.set((grandTotals.scansLatency.get() + lag) / 2);
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

    long lag(RowsChanged changed) {
        return lag(changed.getSmallestCommittedTxId());
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
