package com.jivesoftware.os.amza.service.stats;

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowsChanged;
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

    private final Map<RingHost, AtomicLong> took = new ConcurrentSkipListMap<>();
    private final Map<RingHost, AtomicLong> offered = new ConcurrentHashMap<>();

    private final Map<String, Long> ongoingCompaction = new ConcurrentHashMap<>();
    private final List<Entry<String, Long>> recentCompaction = new ArrayList<>();
    private final AtomicLong totalCompactions = new AtomicLong();

    private final Totals grandTotals = new Totals();
    private final Map<RegionName, Totals> regionTotals = new ConcurrentHashMap<>();

    public AmzaStats() {
    }

    static public class Totals {

        public final AtomicLong gets = new AtomicLong();
        public final AtomicLong getsLag = new AtomicLong();
        public final AtomicLong scans = new AtomicLong();
        public final AtomicLong scansLag = new AtomicLong();
        public final AtomicLong takeUpdates = new AtomicLong();
        public final AtomicLong takeUpdatesLag = new AtomicLong();
        public final AtomicLong replicates = new AtomicLong();
        public final AtomicLong replicatesLag = new AtomicLong();
        public final AtomicLong applies = new AtomicLong();
        public final AtomicLong appliesLag = new AtomicLong();
        public final AtomicLong received = new AtomicLong();
        public final AtomicLong receivedLag = new AtomicLong();
    }

    public void took(RingHost host) {
        AtomicLong got = took.get(host);
        if (got == null) {
            got = new AtomicLong();
            took.put(host, got);
        }
        got.incrementAndGet();
    }

    public long getTotalTakes(RingHost host) {
        AtomicLong got = took.get(host);
        if (got == null) {
            return 0;
        }
        return got.get();
    }

    public void offered(RingHost host) {
        AtomicLong got = offered.get(host);
        if (got == null) {
            got = new AtomicLong();
            offered.put(host, got);
        }
        got.incrementAndGet();
    }

    public long getTotalOffered(RingHost host) {
        AtomicLong got = offered.get(host);
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

    public void took(RingHost from, RowsChanged changed) {
        grandTotals.takeUpdates.addAndGet(changed.getApply().size());
        Totals totals = regionTotals(changed.getRegionName());
        totals.takeUpdates.addAndGet(changed.getApply().size());
        totals.takeUpdatesLag.set(lag(changed));
        logChanges("Took", changed);
    }

    public void replicated(RingHost to, RowsChanged changed) {
        grandTotals.replicates.addAndGet(changed.getApply().size());
        Totals totals = regionTotals(changed.getRegionName());
        totals.replicates.addAndGet(changed.getApply().size());
        totals.replicatesLag.set(lag(changed));
        logChanges("Replicated", changed);
    }

    public void applied(RowsChanged changed) {
        grandTotals.applies.addAndGet(changed.getApply().size());
        Totals totals = regionTotals(changed.getRegionName());
        totals.applies.addAndGet(changed.getApply().size());
        totals.appliesLag.set(lag(changed));
        logChanges("Applied", changed);
    }

    public void received(RowsChanged changed) {
        grandTotals.received.addAndGet(changed.getApply().size());
        Totals totals = regionTotals(changed.getRegionName());
        totals.received.addAndGet(changed.getApply().size());
        totals.receivedLag.set(lag(changed));
        logChanges("Received", changed);
    }

    private Totals regionTotals(RegionName regionName) {
        Totals got = regionTotals.get(regionName);
        if (got == null) {
            got = new Totals();
            regionTotals.put(regionName, got);
        }
        return got;
    }

    public Map<RegionName, Totals> getRegionTotals() {
        return regionTotals;
    }

    private void logChanges(String name, RowsChanged changed) {
        if (!changed.isEmpty()) {
            RegionName regionName = changed.getRegionName();

            LOG.debug("{} {} to region: {}:{} lag:{}", new Object[]{name,
                changed.getApply().size(),
                regionName.getRegionName(),
                regionName.getRingName(),
                lag(changed)});
        }
    }

    long lag(RowsChanged changed) {
        long oldest = changed.getOldestApply();
        if (oldest != Long.MAX_VALUE) {
            long[] unpack = snowflakeIdPacker.unpack(changed.getOldestApply());
            long lag = jiveEpochTimestampProvider.getApproximateTimestamp(System.currentTimeMillis()) - unpack[0];
            return lag;
        }
        return 0;
    }

}
