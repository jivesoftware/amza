package com.jivesoftware.os.amza.service.stats;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.jivesoftware.os.amza.api.IoStats;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.wal.WALCompactionStats;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author jonathan.colt
 */
public class AmzaStats {

    private static final SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
    private static final JiveEpochTimestampProvider jiveEpochTimestampProvider = new JiveEpochTimestampProvider();

    private final Map<RingMember, LongAdder> took = new ConcurrentSkipListMap<>();

    private final Map<CompactionFamily, Map<String, CompactionStats>> ongoingCompaction = Maps.newConcurrentMap();
    private final Map<CompactionFamily, LongAdder> totalCompactions = Maps.newConcurrentMap();
    private final List<Entry<String, CompactionStats>> recentCompaction = new ArrayList<>();
    private final Interner<String> compactionNameInterner = Interners.newStrongInterner();

    public final Map<RingMember, LongAdder> longPolled = new ConcurrentSkipListMap<>();
    public final Map<RingMember, LongAdder> longPollAvailables = new ConcurrentSkipListMap<>();

    private final Totals grandTotals = new Totals();
    private final Map<PartitionName, Totals> partitionTotals = Maps.newConcurrentMap();

    public final Multiset<RingMember> takeErrors = ConcurrentHashMultiset.create();

    public final IoStats loadIoStats = new IoStats();
    public final IoStats getIoStats = new IoStats();
    public final IoStats takeIoStats = new IoStats();
    public final IoStats mergeIoStats = new IoStats();
    public final IoStats updateIoStats = new IoStats();
    public final IoStats compactTombstoneIoStats = new IoStats();


    public final NetStats netStats = new NetStats();

    public final LongAdder addMember = new LongAdder();
    public final LongAdder removeMember = new LongAdder();
    public final LongAdder getRing = new LongAdder();
    public final LongAdder rowsStream = new LongAdder();
    public final LongAdder completedRowsStream = new LongAdder();

    public final LongAdder availableRowsStream = new LongAdder();
    public final LongAdder rowsTaken = new LongAdder();
    public final LongAdder completedRowsTake = new LongAdder();

    public final LongAdder pingsSent = new LongAdder();
    public final LongAdder pingsReceived = new LongAdder();
    public final LongAdder pongsSent = new LongAdder();
    public final LongAdder pongsReceived = new LongAdder();

    public final LongAdder invalidatesSent = new LongAdder();
    public final LongAdder invalidatesReceived = new LongAdder();

    public final LongAdder backPressure = new LongAdder();
    public final LongAdder pushBacks = new LongAdder();

    public long[] deltaStripeMergeLoaded = new long[0];
    public double[] deltaStripeLoad = new double[0];

    public long[] deltaStripeMergePending = new long[0];
    public double[] deltaStripeMerge = new double[0];

    public final LongAdder deltaFirstCheckRemoves = new LongAdder();
    public final LongAdder deltaSecondCheckRemoves = new LongAdder();

    public final LongAdder takes = new LongAdder();
    public final LongAdder takeExcessRows = new LongAdder();

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

        public final LongAdder gets = new LongAdder();
        public volatile long getsLatency = 0;
        public final LongAdder scans = new LongAdder();
        public volatile long scansLatency = 0;
        public final LongAdder scanKeys = new LongAdder();
        public volatile long scanKeysLatency = 0;
        public final LongAdder updates = new LongAdder();
        public volatile long updatesLag = 0;
        public final LongAdder offers = new LongAdder();
        public volatile long offersLag = 0;
        public final Map<RingMember, AtomicLong> memberOffersLag = Maps.newConcurrentMap();
        public final LongAdder takes = new LongAdder();
        public volatile long takesLag = 0;
        public final LongAdder takeApplies = new LongAdder();
        public volatile long takeAppliesLag = 0;
        public final LongAdder directApplies = new LongAdder();
        public volatile long directAppliesLag = 0;
        public final LongAdder acks = new LongAdder();
        public volatile long acksLag = 0;
        public final Map<RingMember, AtomicLong> memberAcksLag = Maps.newConcurrentMap();
        public final LongAdder quorums = new LongAdder();
        public volatile long quorumsLatency = 0;
        public final Map<RingMember, AtomicLong> memberQuorumsLatency = Maps.newConcurrentMap();
        public final LongAdder quorumTimeouts = new LongAdder();
    }

    public void longPolled(RingMember member) {
        longPolled.computeIfAbsent(member, (key) -> new LongAdder()).increment();
    }

    public void longPollAvailables(RingMember member) {
        longPollAvailables.computeIfAbsent(member, (key) -> new LongAdder()).increment();
    }

    public void took(RingMember member) {
        took.computeIfAbsent(member, (key) -> new LongAdder()).increment();
    }

    public long getTotalTakes(RingMember member) {
        LongAdder got = took.get(member);
        if (got == null) {
            return 0;
        }
        return got.longValue();
    }

    public enum CompactionFamily {
        expunge, tombstone, merge, load;
    }

    public int ongoingCompaction(CompactionFamily family) {
        Map<String, CompactionStats> got = ongoingCompaction.computeIfAbsent(family, (key) -> Maps.newConcurrentMap());
        return got.size();
    }

    public CompactionStats beginCompaction(CompactionFamily family, String name) {
        Map<String, CompactionStats> got = ongoingCompaction.computeIfAbsent(family, (key) -> Maps.newConcurrentMap());
        CompactionStats compactionStats = new CompactionStats(family, name, System.currentTimeMillis());
        got.put(name, compactionStats);
        return compactionStats;
    }

    public class CompactionStats implements WALCompactionStats {

        private final CompactionFamily family;
        private final String name;
        private final long startTime;
        private long endTime;

        private final Map<String, Long> counters = Maps.newConcurrentMap();
        private final Map<String, Long> timers = Maps.newConcurrentMap();

        public CompactionStats(CompactionFamily family, String name, long startTime) {
            this.family = family;
            this.name = name;
            this.startTime = startTime;
        }

        public long startTime() {
            return startTime;
        }

        public void finished() {
            this.endTime = System.currentTimeMillis();
            endCompaction(family, name);
        }

        public long elapse() {
            return System.currentTimeMillis() - startTime;
        }

        public long duration() {
            if (endTime == 0) {
                return elapse();
            }
            return endTime - startTime;
        }

        @Override
        public Set<Map.Entry<String, Long>> getTimings() {
            return timers.entrySet();
        }

        @Override
        public Set<Map.Entry<String, Long>> getCounts() {
            return counters.entrySet();
        }

        @Override
        public void add(String name, long count) {
            counters.compute(compactionNameInterner.intern(name), (k, v) -> {
                return v == null ? count : v + count;
            });
        }

        @Override
        public void start(String name) {
            timers.compute(compactionNameInterner.intern(name), (k, v) -> {
                return v == null ? System.currentTimeMillis() : v;
            });
        }

        @Override
        public void stop(String name) {
            timers.compute(compactionNameInterner.intern(name), (k, v) -> {
                return v == null ? null : System.currentTimeMillis() - v;
            });
        }

    }

    private void endCompaction(CompactionFamily family, String name) {
        Map<String, CompactionStats> got = ongoingCompaction.computeIfAbsent(family, (key) -> Maps.newConcurrentMap());
        CompactionStats compactionStats = got.remove(name);

        totalCompactions.computeIfAbsent(family, (key) -> new LongAdder()).increment();
        if (compactionStats != null) {
            compactionStats.finished();
            recentCompaction.add(new AbstractMap.SimpleEntry<>(family + " " + name, compactionStats));
            while (recentCompaction.size() > 1_000) {
                recentCompaction.remove(0);
            }
        }
    }

    public List<Entry<String, CompactionStats>> recentCompaction() {
        return recentCompaction;
    }

    public List<Entry<String, CompactionStats>> ongoingCompactions(CompactionFamily... families) {
        List<Entry<String, CompactionStats>> ongoing = new ArrayList<>();
        for (CompactionFamily family : families) {
            Map<String, CompactionStats> got = ongoingCompaction.get(family);
            if (got != null) {
                for (Entry<String, CompactionStats> e : got.entrySet()) {
                    ongoing.add(new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()));
                }
            }
        }
        return ongoing;
    }

    public long getTotalCompactions(CompactionFamily family) {
        return totalCompactions.computeIfAbsent(family, (key) -> new LongAdder()).longValue();
    }

    public Totals getGrandTotal() {
        return grandTotals;
    }

    public void updates(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.updates.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.updates.add(count);
        if (smallestTxId != -1) {
            long lag = lag(smallestTxId);
            totals.updatesLag = lag;
            grandTotals.updatesLag = (grandTotals.updatesLag + lag) / 2;
        }
    }

    public void offers(RingMember to, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.offers.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.offers.add(count);
        if (smallestTxId != -1) {
            long lag = lag(smallestTxId);
            totals.offersLag = lag;
            grandTotals.offersLag = (grandTotals.offersLag + lag) / 2;
            grandTotals.memberOffersLag.computeIfAbsent(to, ringMember1 -> new AtomicLong())
                .accumulateAndGet(lag, (left, right) -> (left + right) / 2);
        }
    }

    public void acks(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.acks.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.acks.add(count);
        if (smallestTxId != -1) {
            long lag = lag(smallestTxId);
            totals.acksLag = lag;
            grandTotals.acksLag = (grandTotals.acksLag + lag) / 2;
            grandTotals.memberAcksLag.computeIfAbsent(from, ringMember1 -> new AtomicLong())
                .accumulateAndGet(lag, (left, right) -> (left + right) / 2);
        }
    }

    public void quorums(PartitionName partitionName, int count, long lag, List<RingMember> tookFrom) {
        grandTotals.quorums.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.quorums.add(count);
        totals.quorumsLatency = lag;
        grandTotals.quorumsLatency = (grandTotals.quorumsLatency + lag) / 2;
        for (RingMember ringMember : tookFrom) {
            grandTotals.memberQuorumsLatency.computeIfAbsent(ringMember, ringMember1 -> new AtomicLong())
                .accumulateAndGet(lag, (left, right) -> (left + right) / 2);
        }
    }

    public void quorumTimeouts(PartitionName partitionName, int count) {
        grandTotals.quorumTimeouts.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.quorumTimeouts.add(count);
    }

    public void took(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.takes.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.takes.add(count);
        if (smallestTxId != -1) {
            long lag = lag(smallestTxId);
            totals.takesLag = lag;
            grandTotals.takesLag = (grandTotals.takesLag + lag) / 2;
        }
    }

    public void tookApplied(RingMember from, PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.takeApplies.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.takeApplies.add(count);
        if (smallestTxId != -1) {
            long lag = lag(smallestTxId);
            totals.takeAppliesLag = lag;
            grandTotals.takeAppliesLag = (grandTotals.takeAppliesLag + lag) / 2;
        }
    }

    public void direct(PartitionName partitionName, int count, long smallestTxId) {
        grandTotals.directApplies.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.directApplies.add(count);
        if (smallestTxId != -1) {
            long lag = lag(smallestTxId);
            totals.directAppliesLag = lag;
            grandTotals.directAppliesLag = (grandTotals.directAppliesLag + lag) / 2;
        }
    }

    public void gets(PartitionName partitionName, int count, long lag) {
        grandTotals.gets.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.gets.add(count);
        totals.getsLatency = lag;
        grandTotals.getsLatency = (grandTotals.getsLatency + lag) / 2;
    }

    public void scans(PartitionName partitionName, int count, long lag) {
        grandTotals.scans.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.scans.add(count);
        totals.scansLatency = lag;
        grandTotals.scansLatency = (grandTotals.scansLatency + lag) / 2;
    }

    public void scanKeys(PartitionName partitionName, int count, long lag) {
        grandTotals.scanKeys.add(count);
        Totals totals = partitionTotals(partitionName);
        totals.scanKeys.add(count);
        totals.scanKeysLatency = lag;
        grandTotals.scanKeysLatency = (grandTotals.scanKeysLatency + lag) / 2;
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

    long lag(long oldest) {
        if (oldest != Long.MAX_VALUE) {
            long[] unpack = snowflakeIdPacker.unpack(oldest);
            long lag = jiveEpochTimestampProvider.getApproximateTimestamp(System.currentTimeMillis()) - unpack[0];
            return lag;
        }
        return 0;
    }

}
