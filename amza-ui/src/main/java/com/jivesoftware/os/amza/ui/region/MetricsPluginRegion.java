package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.Partition;
import com.jivesoftware.os.amza.service.partition.RemoteVersionedState;
import com.jivesoftware.os.amza.service.replication.PartitionStripe;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionFamily;
import com.jivesoftware.os.amza.service.stats.AmzaStats.Totals;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.ui.utils.MinMaxLong;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.mlogger.core.LoggerSummary;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 *
 */
// soy.page.healthPluginRegion
public class MetricsPluginRegion implements PageRegion<MetricsPluginRegion.MetricsPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private final String template;
    private final String partitionMetricsTemplate;
    private final String statsTemplate;
    private final String visualizePartitionTemplate;
    private final SoyRenderer renderer;
    private final AmzaRingReader ringReader;
    private final AmzaService amzaService;
    private final AmzaStats amzaStats;

    private final List<GarbageCollectorMXBean> garbageCollectors;
    private final MemoryMXBean memoryBean;
    private final RuntimeMXBean runtimeBean;

    public MetricsPluginRegion(String template,
        String partitionMetricsTemplate,
        String statsTemplate,
        String visualizePartitionTemplate,
        SoyRenderer renderer,
        AmzaRingReader ringReader,
        AmzaService amzaService,
        AmzaStats amzaStats) {
        this.template = template;
        this.partitionMetricsTemplate = partitionMetricsTemplate;
        this.statsTemplate = statsTemplate;
        this.visualizePartitionTemplate = visualizePartitionTemplate;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.amzaService = amzaService;
        this.amzaStats = amzaStats;

        garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
        memoryBean = ManagementFactory.getMemoryMXBean();
        runtimeBean = ManagementFactory.getRuntimeMXBean();

    }

    public static class MetricsPluginRegionInput {

        final String ringName;
        final String partitionName;
        final boolean visualize;

        public MetricsPluginRegionInput(String ringName, String partitionName, boolean visualize) {
            this.ringName = ringName;
            this.partitionName = partitionName;
            this.visualize = visualize;
        }
    }

    class VisualizePartition implements RowStream {

        List<Run> runs = new ArrayList<>();
        Run lastRun;
        long rowCount;

        @Override
        public boolean row(long rowFP, long rowTxId, RowType rowType, byte[] row) throws Exception {
            rowCount++;
            String subType = null;
            if (rowType == RowType.system) {
                long[] parts = UIO.bytesLongs(row);
                if (parts[0] == 0) {
                    MinMaxLong minMaxLong = new MinMaxLong();
                    for (int i = 3; i < parts.length; i++) {
                        minMaxLong.value(parts[i]);
                    }
                    subType = "compactionHint:  newCount=" + parts[1]
                        + " clobberCount=" + parts[2]
                        + " min:" + Long.toHexString(minMaxLong.min())
                        + " max:" + Long.toHexString(minMaxLong.max());
                }
                if (parts[0] == 1) {
                    subType = "indexCommit: txId=" + Long.toHexString(parts[1]);
                }
                if (parts[0] == 2) {
                    subType = "leaps: lastTx:" + Long.toHexString(parts[1]);
                }
            }
            if (lastRun == null || lastRun.rowType != rowType || subType != null) {
                if (lastRun != null) {
                    runs.add(lastRun);
                }
                lastRun = new Run(rowType, rowFP, rowTxId);
            }

            if (subType != null) {
                lastRun.subType = subType;
            }
            lastRun.bytes += row.length;
            lastRun.count++;
            return true;
        }

        public void done(Map<String, Object> data) {
            runs.add(lastRun);

            List<Map<String, String>> runMaps = new ArrayList<>();
            for (Run r : runs) {
                Map<String, String> run = new HashMap<>();
                run.put("rowType", r.rowType.name());
                run.put("subType", r.subType);
                run.put("startFp", String.valueOf(r.startFp));
                run.put("rowTxId", Long.toHexString(r.rowTxId));
                run.put("bytes", numberFormat.format(r.bytes));
                run.put("count", numberFormat.format(r.count));
                runMaps.add(run);
            }
            data.put("runs", runMaps);
            data.put("rowCount", numberFormat.format(rowCount));
        }

        class Run {

            RowType rowType;
            String subType = "";
            long startFp;
            long rowTxId;
            long bytes;
            long count;

            public Run(RowType rowType, long startFp, long rowTxId) {
                this.rowType = rowType;
                this.startFp = startFp;
                this.rowTxId = rowTxId;
            }

        }

    }

    @Override
    public String render(MetricsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            if (input.partitionName.length() > 0) {
                data.put("ringName", input.ringName);
                data.put("partitionName", input.partitionName);
                if (input.visualize) {
                    VisualizePartition visualizePartition = new VisualizePartition();
                    amzaService.visualizePartition(input.ringName.getBytes(), input.partitionName.getBytes(), visualizePartition);
                    visualizePartition.done(data);
                    return renderer.render(visualizePartitionTemplate, data);
                } else {
                    return renderer.render(partitionMetricsTemplate, data);
                }
            } else {

                data.put("addMember", numberFormat.format(amzaStats.addMember.get()));
                data.put("removeMember", numberFormat.format(amzaStats.removeMember.get()));
                data.put("getRing", numberFormat.format(amzaStats.getRing.get()));
                data.put("rowsStream", numberFormat.format(amzaStats.rowsStream.get()));
                data.put("availableRowsStream", numberFormat.format(amzaStats.availableRowsStream.get()));
                data.put("rowsTaken", numberFormat.format(amzaStats.rowsTaken.get()));

                List<Map<String, String>> longPolled = new ArrayList<>();
                for (Entry<RingMember, AtomicLong> polled : amzaStats.longPolled.entrySet()) {
                    AtomicLong longPollAvailables = amzaStats.longPollAvailables.get(polled.getKey());
                    longPolled.add(ImmutableMap.of("member", polled.getKey().getMember(),
                        "longPolled", numberFormat.format(polled.getValue().get()),
                        "longPollAvailables", numberFormat.format(longPollAvailables == null ? -1L : longPollAvailables.get())));
                }

                data.put("longPolled", longPolled);

                data.put("grandTotals", regionTotals(null, amzaStats.getGrandTotal(), false));
                List<Map<String, Object>> regionTotals = new ArrayList<>();
                List<PartitionName> partitionNames = Lists.newArrayList(amzaService.getMemberPartitionNames());
                Collections.sort(partitionNames);
                for (PartitionName partitionName : partitionNames) {
                    Totals totals = amzaStats.getPartitionTotals().get(partitionName);
                    if (totals == null) {
                        totals = new Totals();
                    }
                    regionTotals.add(regionTotals(partitionName, totals, false));
                }
                data.put("regionTotals", regionTotals);
            }
            return renderer.render(template, data);
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
            return "Error";
        }

    }

    public String renderStats(String filter) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            data.put("grandTotals", regionTotals(null, amzaStats.getGrandTotal(), true));
            List<Map<String, Object>> regionTotals = new ArrayList<>();
            List<PartitionName> partitionNames = Lists.newArrayList(amzaService.getMemberPartitionNames());
            Collections.sort(partitionNames);
            for (PartitionName partitionName : partitionNames) {
                if (new String(partitionName.getName()).contains(filter)) {
                    Totals totals = amzaStats.getPartitionTotals().get(partitionName);
                    if (totals == null) {
                        totals = new Totals();
                    }
                    regionTotals.add(regionTotals(partitionName, totals, true));
                }
            }
            data.put("regionTotals", regionTotals);
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }
        return renderer.render(statsTemplate, data);
    }

    public Map<String, Object> regionTotals(PartitionName name, AmzaStats.Totals totals, boolean includeCount) throws Exception {
        Map<String, Object> map = new HashMap<>();
        if (name != null) {

            map.put("type", name.isSystemPartition() ? "SYSTEM" : "USER");
            map.put("name", new String(name.getName()));
            map.put("ringName", new String(name.getRingName()));
            RingTopology ring = ringReader.getRing(name.getRingName());
            List<Map<String, String>> ringMaps = new ArrayList<>();
            for (RingMemberAndHost entry : ring.entries) {
                ringMaps.add(ImmutableMap.of("member", entry.ringMember.getMember(),
                    "host", entry.ringHost.getHost(),
                    "port", String.valueOf(entry.ringHost.getPort())));
            }
            map.put("ring", ringMaps);

            Partition partition = amzaService.getPartition(name);
            if (includeCount) {
                map.put("count", numberFormat.format(partition.count()));
            } else {
                map.put("count", "(requires watch)");
            }

            partition.highestTxId((versionedAquarium, highestTxId) -> {
                VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                LivelyEndState livelyEndState = versionedAquarium.getLivelyEndState();
                State currentState = livelyEndState.getCurrentState();
                map.put("version", Long.toHexString(versionedPartitionName.getPartitionVersion()));
                map.put("state", currentState != null ? currentState.name() : "unknown");
                map.put("isOnline", livelyEndState.isOnline());
                map.put("highestTxId", Long.toHexString(highestTxId));

                if (includeCount) {
                    if (name.isSystemPartition()) {
                        HighwaterStorage systemHighwaterStorage = amzaService.getSystemHighwaterStorage();
                        WALHighwater partitionHighwater = systemHighwaterStorage.getPartitionHighwater(
                            new VersionedPartitionName(name, versionedPartitionName.getPartitionVersion()));
                        map.put("highwaters", renderHighwaters(partitionHighwater));
                    } else {
                        PartitionStripeProvider partitionStripeProvider = amzaService.getPartitionStripeProvider();
                        partitionStripeProvider.txPartition(name, (PartitionStripe stripe, HighwaterStorage highwaterStorage) -> {
                            WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(
                                new VersionedPartitionName(name, versionedPartitionName.getPartitionVersion()));
                            map.put("highwaters", renderHighwaters(partitionHighwater));
                            return null;
                        });
                    }
                } else {
                    map.put("highwaters", "(requires watch)");
                }

                map.put("localState", ImmutableMap.of("online", livelyEndState.isOnline(),
                    "state", currentState != null ? currentState.name() : "unknown",
                    "name", new String(amzaService.getRingReader().getRingMember().asAquariumMember().getMember()),
                    "partitionVersion", String.valueOf(versionedPartitionName.getPartitionVersion()),
                    "stripeVersion", String.valueOf(versionedAquarium.getStripeVersion())));

                List<Map<String, Object>> neighborStates = new ArrayList<>();
                Set<RingMember> neighboringRingMembers = amzaService.getRingReader().getNeighboringRingMembers(name.getRingName());
                for (RingMember ringMember : neighboringRingMembers) {

                    RemoteVersionedState neighborState = amzaService.getPartitionStateStorage().getRemoteVersionedState(ringMember, name);
                    neighborStates.add(ImmutableMap.of("version", neighborState != null ? String.valueOf(neighborState.version) : "unknown",
                        "state", neighborState != null && neighborState.waterline != null ? neighborState.waterline.getState().name() : "unknown",
                        "name", new String(ringMember.getMember().getBytes())));
                }
                map.put("neighborStates", neighborStates);

                return null;
            });
        }
        map.put("gets", numberFormat.format(totals.gets.get()));
        map.put("getsLag", getDurationBreakdown(totals.getsLag.get()));
        map.put("scans", numberFormat.format(totals.scans.get()));
        map.put("scansLag", getDurationBreakdown(totals.scansLag.get()));
        map.put("directApplies", numberFormat.format(totals.directApplies.get()));
        map.put("directAppliesLag", getDurationBreakdown(totals.directAppliesLag.get()));
        map.put("updates", numberFormat.format(totals.updates.get()));
        map.put("updatesLag", getDurationBreakdown(totals.updatesLag.get()));
        map.put("offers", numberFormat.format(totals.offers.get()));
        map.put("offersLag", getDurationBreakdown(totals.offersLag.get()));
        map.put("takes", numberFormat.format(totals.takes.get()));
        map.put("takesLag", getDurationBreakdown(totals.takesLag.get()));
        map.put("takeApplies", numberFormat.format(totals.takeApplies.get()));
        map.put("takeAppliesLag", getDurationBreakdown(totals.takeAppliesLag.get()));

        return map;
    }

    public String renderHighwaters(WALHighwater walHighwater) {
        StringBuilder sb = new StringBuilder();
        for (WALHighwater.RingMemberHighwater e : walHighwater.ringMemberHighwater) {
            sb.append("<p>");
            sb.append(e.ringMember.getMember()).append("=").append(Long.toHexString(e.transactionId)).append("\n");
            sb.append("</p>");
        }
        return sb.toString();
    }

    public Map<String, Object> renderPartition(PartitionName partitionName, long startFp, long endFp) {
        Map<String, Object> partitionViz = new HashMap<>();
        //amzaService.getPartitionCreator().

        return partitionViz;
    }

    public String renderOverview() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("<p>uptime<span class=\"badge\">").append(getDurationBreakdown(runtimeBean.getUptime())).append("</span>");
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;diskR<span class=\"badge\">").append(humanReadableByteCount(amzaStats.ioStats.read.get(), false)).append("</span>");
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;diskW<span class=\"badge\">").append(humanReadableByteCount(amzaStats.ioStats.wrote.get(), false)).append("</span>");
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;netR<span class=\"badge\">").append(humanReadableByteCount(amzaStats.netStats.read.get(), false)).append("</span>");
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;netW<span class=\"badge\">").append(humanReadableByteCount(amzaStats.netStats.wrote.get(), false)).append("</span>");
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;deltaRem1<span class=\"badge\">").append(amzaStats.deltaFirstCheckRemoves.get()).append("</span>");
        sb.append("&nbsp;&nbsp;&nbsp;&nbsp;deltaRem2<span class=\"badge\">").append(amzaStats.deltaSecondCheckRemoves.get()).append("</span>");

        double processCpuLoad = getProcessCpuLoad();
        sb.append(progress("CPU",
            (int) (processCpuLoad),
            numberFormat.format(processCpuLoad) + " cpu load"));

        double memoryLoad = (double) memoryBean.getHeapMemoryUsage().getUsed() / (double) memoryBean.getHeapMemoryUsage().getMax();
        sb.append(progress("Heap",
            (int) (memoryLoad * 100),
            humanReadableByteCount(memoryBean.getHeapMemoryUsage().getUsed(), false)
                + " used out of " + humanReadableByteCount(memoryBean.getHeapMemoryUsage().getMax(), false)));

        long s = 0;
        for (GarbageCollectorMXBean gc : garbageCollectors) {
            s += gc.getCollectionTime();
        }
        double gcLoad = (double) s / (double) runtimeBean.getUptime();
        sb.append(progress("GC",
            (int) (gcLoad * 100),
            getDurationBreakdown(s) + " total gc"));

        Totals grandTotal = amzaStats.getGrandTotal();

        sb.append(progress("Gets (" + numberFormat.format(grandTotal.gets.get()) + ")",
            (int) (((double) grandTotal.getsLag.longValue() / 1000d) * 100),
            getDurationBreakdown(grandTotal.getsLag.longValue()) + " lag"));

        sb.append(progress("Scans (" + numberFormat.format(grandTotal.scans.get()) + ")",
            (int) (((double) grandTotal.scansLag.longValue() / 1000d) * 100),
            getDurationBreakdown(grandTotal.scansLag.longValue()) + " lag"));

        sb.append(progress("Direct Applied (" + numberFormat.format(grandTotal.directApplies.get()) + ")",
            (int) (((double) grandTotal.directAppliesLag.longValue() / 1000d) * 100),
            getDurationBreakdown(grandTotal.directAppliesLag.longValue()) + " lag"));

        sb.append(progress("Updates (" + numberFormat.format(grandTotal.updates.get()) + ")",
            (int) (((double) grandTotal.updatesLag.longValue() / 10000d) * 100),
            getDurationBreakdown(grandTotal.updatesLag.longValue()) + " lag"));

        sb.append(progress("Offers (" + numberFormat.format(grandTotal.offers.get()) + ")",
            (int) (((double) grandTotal.offersLag.longValue() / 10000d) * 100),
            getDurationBreakdown(grandTotal.offersLag.longValue()) + " lag"));

        sb.append(progress("Took (" + numberFormat.format(grandTotal.takes.get()) + ")",
            (int) (((double) grandTotal.takesLag.longValue() / 10000d) * 100),
            getDurationBreakdown(grandTotal.takesLag.longValue()) + " lag"));

        sb.append(progress("Took Applied (" + numberFormat.format(grandTotal.takeApplies.get()) + ")",
            (int) (((double) grandTotal.takeAppliesLag.longValue() / 1000d) * 100),
            getDurationBreakdown(grandTotal.takeAppliesLag.longValue()) + " lag"));

        sb.append(progress("Active Long Polls (" + numberFormat.format(amzaStats.availableRowsStream.get()) + ")",
            (int) (((double) amzaStats.availableRowsStream.get() / 100d) * 100), ""));

        sb.append(progress("Active Row Streaming (" + numberFormat.format(amzaStats.rowsStream.get()) + ")",
            (int) (((double) amzaStats.rowsStream.get() / 100d) * 100), "" + numberFormat.format(amzaStats.completedRowsStream.get())));

        sb.append(progress("Active Row Acknowledging (" + numberFormat.format(amzaStats.rowsTaken.get()) + ")",
            (int) (((double) amzaStats.rowsTaken.get() / 100d) * 100), "" + numberFormat.format(amzaStats.completedRowsTake.get())));

        sb.append(progress("Back Pressure (" + numberFormat.format(amzaStats.backPressure.get()) + ")",
            (int) (((double) amzaStats.backPressure.get() / 10000d) * 100), "" + amzaStats.pushBacks.get()));

        long[] count = amzaStats.deltaStripeMergeLoaded;
        double[] load = amzaStats.deltaStripeLoad;
        long[] mergeCount = amzaStats.deltaStripeMergePending;
        double[] mergeLoad = amzaStats.deltaStripeMerge;
        for (int i = 0; i < load.length; i++) {
            sb.append(progress(" Delta Stripe " + i + " (" + load[i] + ")", (int) (load[i] * 100), "" + numberFormat.format(count[i])));
            if (mergeLoad.length > i && mergeCount.length > i) {
                sb.append(progress("Merge Stripe " + i + " (" + numberFormat.format(mergeLoad[i]) + ")", (int) (mergeLoad[i] * 100),
                    numberFormat.format(mergeCount[i]) + " partitions"));
            }
        }

        int tombostoneCompaction = amzaStats.ongoingCompaction(AmzaStats.CompactionFamily.tombstone);
        int mergeCompaction = amzaStats.ongoingCompaction(AmzaStats.CompactionFamily.merge);
        int expungeCompaction = amzaStats.ongoingCompaction(AmzaStats.CompactionFamily.expunge);

        sb.append(progress("Tombstone Compactions (" + numberFormat.format(tombostoneCompaction) + ")",
            (int) (((double) tombostoneCompaction / 10d) * 100), " total:" + amzaStats.getTotalCompactions(CompactionFamily.tombstone)));

        sb.append(progress("Merge Compactions (" + numberFormat.format(mergeCompaction) + ")",
            (int) (((double) mergeCompaction / 10d) * 100), " total:" + amzaStats.getTotalCompactions(CompactionFamily.merge)));

        sb.append(progress("Expunge Compactions (" + numberFormat.format(expungeCompaction) + ")",
            (int) (((double) expungeCompaction / 10d) * 100), " total:" + amzaStats.getTotalCompactions(CompactionFamily.expunge)));

        sb.append("<p><pre>");
        for (String l : LoggerSummary.INSTANCE.lastNErrors.get()) {
            sb.append("ERROR ").append(l).append("\n");
        }
        for (String l : LoggerSummary.INSTANCE.lastNWarns.get()) {
            sb.append("WARN ").append(l).append("\n");
        }
        for (String l : LoggerSummary.INSTANCE.lastNInfos.get()) {
            sb.append("INFO ").append(l).append("\n");
        }
        sb.append("</pre><p>");

        return sb.toString();
    }

    private String progress(String title, int progress, String value) {
        Map<String, Object> data = new HashMap<>();
        data.put("title", title);
        data.put("progress", progress);
        data.put("value", value);
        return renderer.render("soy.page.amzaStackedProgress", data);
    }

    public static double getProcessCpuLoad() throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[] { "ProcessCpuLoad" });

        if (list.isEmpty()) {
            return Double.NaN;
        }

        Attribute att = (Attribute) list.get(0);
        Double value = (Double) att.getValue();

        if (value == -1.0) {
            return 0;  // usually takes a couple of seconds before we get real values
        }
        return ((int) (value * 1000) / 10.0);        // returns a percentage value with 1 decimal point precision
    }

    @Override
    public String getTitle() {
        return "Metrics";
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static String getDurationBreakdown(long millis) {
        if (millis < 0) {
            return String.valueOf(millis);
        }

        long days = TimeUnit.MILLISECONDS.toDays(millis);
        millis -= TimeUnit.DAYS.toMillis(days);
        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        millis -= TimeUnit.SECONDS.toMillis(seconds);

        StringBuilder sb = new StringBuilder(64);
        boolean showRemaining = false;
        if (showRemaining || days > 0) {
            sb.append(days);
            sb.append(" Days ");
            showRemaining = true;
        }
        if (showRemaining || hours > 0) {
            if (hours < 10) {
                sb.append('0');
            }
            sb.append(hours);
            sb.append(":");
            showRemaining = true;
        }
        if (showRemaining || minutes > 0) {
            if (minutes < 10) {
                sb.append('0');
            }
            sb.append(minutes);
            sb.append(":");
            showRemaining = true;
        }
        if (showRemaining || seconds > 0) {
            if (seconds < 10) {
                sb.append('0');
            }
            sb.append(seconds);
            sb.append(".");
            showRemaining = true;
        }
        if (millis < 100) {
            sb.append('0');
        }
        if (millis < 10) {
            sb.append('0');
        }
        sb.append(millis);

        return (sb.toString());
    }

}
