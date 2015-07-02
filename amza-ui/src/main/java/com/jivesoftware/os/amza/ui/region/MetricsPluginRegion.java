package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.stats.AmzaStats.Totals;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.LoggerSummary;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
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

    private final String template;
    private final String partitionMetricsTemplate;
    private final String statsTemplate;
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
        SoyRenderer renderer,
        AmzaRingReader ringReader,
        AmzaService amzaService,
        AmzaStats amzaStats) {
        this.template = template;
        this.partitionMetricsTemplate = partitionMetricsTemplate;
        this.statsTemplate = statsTemplate;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.amzaService = amzaService;
        this.amzaStats = amzaStats;

        garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
        memoryBean = ManagementFactory.getMemoryMXBean();
        runtimeBean = ManagementFactory.getRuntimeMXBean();

    }

    public static class MetricsPluginRegionInput {

        final String partitionName;

        public MetricsPluginRegionInput(String partitionName) {
            this.partitionName = partitionName;
        }
    }

    @Override
    public String render(MetricsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            if (input.partitionName.length() > 0) {
                data.put("partitionName", input.partitionName);
                return renderer.render(partitionMetricsTemplate, data);
            } else {

                List<Map<String, String>> longPolled = new ArrayList<>();
                for (Entry<RingMember, AtomicLong> polled : amzaStats.longPolled.entrySet()) {
                    AtomicLong longPollAvailables = amzaStats.longPollAvailables.get(polled.getKey());
                    longPolled.add(ImmutableMap.of("member", polled.getKey().getMember(),
                        "longPolled", String.valueOf(polled.getValue().get()),
                        "longPollAvailables", String.valueOf(longPollAvailables == null ? "-1" : longPollAvailables.get())));
                }

                data.put("longPolled", longPolled);

                data.put("grandTotals", regionTotals(null, amzaStats.getGrandTotal()));
                List<Map<String, Object>> regionTotals = new ArrayList<>();
                ArrayList<PartitionName> partitionNames = new ArrayList<>(amzaService.getPartitionNames());
                Collections.sort(partitionNames);
                for (PartitionName partitionName : partitionNames) {
                    Totals totals = amzaStats.getPartitionTotals().get(partitionName);
                    if (totals == null) {
                        totals = new Totals();
                    }
                    regionTotals.add(regionTotals(partitionName, totals));
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
            data.put("grandTotals", regionTotals(null, amzaStats.getGrandTotal()));
            List<Map<String, Object>> regionTotals = new ArrayList<>();
            ArrayList<PartitionName> partitionNames = new ArrayList<>(amzaService.getPartitionNames());
            Collections.sort(partitionNames);
            for (PartitionName partitionName : partitionNames) {
                if (partitionName.getName().contains(filter)) {
                    Totals totals = amzaStats.getPartitionTotals().get(partitionName);
                    if (totals == null) {
                        totals = new Totals();
                    }
                    regionTotals.add(regionTotals(partitionName, totals));
                }
            }
            data.put("regionTotals", regionTotals);
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }
        return renderer.render(statsTemplate, data);
    }

    public Map<String, Object> regionTotals(PartitionName name, AmzaStats.Totals totals) throws Exception {
        Map<String, Object> map = new HashMap<>();
        if (name != null) {
            map.put("name", name.getName());
            map.put("ringName", name.getRingName());
            NavigableMap<RingMember, RingHost> ring = ringReader.getRing(name.getRingName());
            List<Map<String, String>> ringMaps = new ArrayList<>();
            for (Entry<RingMember, RingHost> r : ring.entrySet()) {
                ringMaps.add(ImmutableMap.of("member", r.getKey().getMember(),
                    "host", r.getValue().getHost(),
                    "port", String.valueOf(r.getValue().getPort())));
            }
            map.put("ring", ringMaps);

            AmzaPartitionAPI partition = amzaService.getPartition(name);
            map.put("count", String.valueOf(partition.count()));
        }
        map.put("gets", String.valueOf(totals.gets.get()));
        map.put("getsLag", String.valueOf(totals.getsLag.get()));
        map.put("scans", String.valueOf(totals.scans.get()));
        map.put("scansLag", String.valueOf(totals.scansLag.get()));
        map.put("directApplies", String.valueOf(totals.directApplies.get()));
        map.put("directAppliesLag", String.valueOf(totals.directAppliesLag.get()));
        map.put("updates", String.valueOf(totals.updates.get()));
        map.put("updatesLag", String.valueOf(totals.updatesLag.get()));
        map.put("offers", String.valueOf(totals.offers.get()));
        map.put("offersLag", String.valueOf(totals.offersLag.get()));
        map.put("takes", String.valueOf(totals.takes.get()));
        map.put("takesLag", String.valueOf(totals.takesLag.get()));
        map.put("takeApplies", String.valueOf(totals.takeApplies.get()));
        map.put("takeAppliesLag", String.valueOf(totals.takeAppliesLag.get()));

        return map;
    }

    public String renderOverview() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("<p>uptime<span class=\"badge\">" + getDurationBreakdown(runtimeBean.getUptime()) + "</span>");
        sb.append("&nbsp&nbsp&nbsp&nbspdiskR<span class=\"badge\">" + humanReadableByteCount(amzaStats.ioStats.read.get(), false) + "</span>");
        sb.append("&nbsp&nbsp&nbsp&nbspdiskW<span class=\"badge\">" + humanReadableByteCount(amzaStats.ioStats.wrote.get(), false) + "</span>");
        sb.append("&nbsp&nbsp&nbsp&nbspnetR<span class=\"badge\">" + humanReadableByteCount(amzaStats.netStats.read.get(), false) + "</span>");
        sb.append("&nbsp&nbsp&nbsp&nbspnetW<span class=\"badge\">" + humanReadableByteCount(amzaStats.netStats.wrote.get(), false) + "</span>");

        double processCpuLoad = getProcessCpuLoad();
        sb.append(progress("CPU",
            (int) (processCpuLoad),
            String.valueOf(processCpuLoad) + " cpu load"));

        double memoryLoad = (double) memoryBean.getHeapMemoryUsage().getUsed() / (double) memoryBean.getHeapMemoryUsage().getMax();
        sb.append(progress("Heap",
            (int) (memoryLoad * 100),
            String.valueOf(humanReadableByteCount(memoryBean.getHeapMemoryUsage().getUsed(), false)
                + " used out of " + humanReadableByteCount(memoryBean.getHeapMemoryUsage().getMax(), false))));

        long s = 0;
        for (GarbageCollectorMXBean gc : garbageCollectors) {
            s += gc.getCollectionTime();
        }
        double gcLoad = (double) s / (double) runtimeBean.getUptime();
        sb.append(progress("GC",
            (int) (gcLoad * 100),
            getDurationBreakdown(s) + " total gc"));

        Totals grandTotal = amzaStats.getGrandTotal();

        sb.append(progress("Gets (" + grandTotal.gets + ")",
            (int) (((double) grandTotal.getsLag.longValue() / 1000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.getsLag.longValue())) + " lag"));

        sb.append(progress("Scans (" + grandTotal.scans + ")",
            (int) (((double) grandTotal.scansLag.longValue() / 1000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.scansLag.longValue())) + " lag"));

        sb.append(progress("Direct Applied (" + grandTotal.directApplies + ")",
            (int) (((double) grandTotal.directAppliesLag.longValue() / 1000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.directAppliesLag.longValue())) + " lag"));

        sb.append(progress("Updates (" + grandTotal.updates + ")",
            (int) (((double) grandTotal.updatesLag.longValue() / 10000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.updatesLag.longValue())) + " lag"));

        sb.append(progress("Offers (" + grandTotal.offers + ")",
            (int) (((double) grandTotal.offersLag.longValue() / 10000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.offersLag.longValue())) + " lag"));

        sb.append(progress("Took Applied (" + grandTotal.takeApplies + ")",
            (int) (((double) grandTotal.takeAppliesLag.longValue() / 1000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.takeAppliesLag.longValue())) + " lag"));

        sb.append(progress("Took (" + grandTotal.takes + ")",
            (int) (((double) grandTotal.takesLag.longValue() / 10000d) * 100),
            String.valueOf(getDurationBreakdown(grandTotal.takesLag.longValue())) + " lag"));

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
        AttributeList list = mbs.getAttributes(name, new String[]{"ProcessCpuLoad"});

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
