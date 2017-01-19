package com.jivesoftware.os.amza.sync.deployable.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.sync.api.AmzaSyncSenderConfig;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSender;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncSenders;
import com.jivesoftware.os.amza.sync.deployable.AmzaSyncStats;
import com.jivesoftware.os.amza.ui.region.PageRegion;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.LoggerSummary;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaAdminRegion implements PageRegion<Void> {

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaSyncStats stats;
    private final boolean senderEnabled;
    private final boolean receiverEnabled;
    private final AmzaSyncSenders syncSenders;

    public AmzaAdminRegion(String template,
        SoyRenderer renderer,
        AmzaSyncStats stats,
        boolean senderEnabled,
        boolean receiverEnabled,
        AmzaSyncSenders syncSenders) {
        this.template = template;
        this.renderer = renderer;
        this.stats = stats;
        this.senderEnabled = senderEnabled;
        this.receiverEnabled = receiverEnabled;
        this.syncSenders = syncSenders;
    }

    @Override
    public String render(Void input) {
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("sender", senderEnabled);
        data.put("receiver", receiverEnabled);
        if (senderEnabled) {
            Collection<AmzaSyncSender> senders = syncSenders.getActiveSenders();
            List<Map<String, Object>> senderData = Lists.newArrayList();
            for (AmzaSyncSender sender : senders) {
                AmzaSyncSenderConfig config = sender.getConfig();
                senderData.add(ImmutableMap.<String, Object>builder()
                    .put("name", config.name)
                    .put("enabled", config.enabled)
                    .put("senderScheme", config.senderScheme)
                    .put("senderHost", config.senderHost)
                    .put("senderPort", config.senderPort)
                    .put("allowSelfSignedCerts", config.allowSelfSignedCerts)
                    .put("syncIntervalMillis", String.valueOf(config.syncIntervalMillis))
                    .put("batchSize", config.batchSize)
                    .build());
            }


            data.put("senders", senderData);

        }

        data.put("errors", String.valueOf(LoggerSummary.INSTANCE.errors.longValue() + LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.errors.longValue()));
        data.put("recentErrors", recentLogs(LoggerSummary.INSTANCE.lastNErrors.get(), LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.lastNErrors.get()));

        data.put("warns", String.valueOf(LoggerSummary.INSTANCE.warns.longValue() + LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.warns.longValue()));
        data.put("recentWarns", recentLogs(LoggerSummary.INSTANCE.lastNWarns.get(), LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.lastNWarns.get()));

        data.put("infos", String.valueOf(LoggerSummary.INSTANCE.infos.longValue() + LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.infos.longValue()));
        data.put("recentInfos", recentLogs(LoggerSummary.INSTANCE.lastNInfos.get(), LoggerSummary.INSTANCE_EXTERNAL_INTERACTIONS.lastNInfos.get()));

        ingressed(data);
        egressed(data);

        return renderer.render(template, data);
    }

    private List<String> recentLogs(String[] internal, String[] external) {
        List<String> log = new ArrayList<>();
        if (internal != null) {
            for (String i : internal) {
                if (i != null) {
                    log.add(i);
                }
            }
        }
        if (external != null) {
            for (String e : external) {
                if (e != null) {
                    log.add(e);
                }
            }
        }
        return log;

    }

    private void ingressed(Map<String, Object> data) {
        List<Map<String, String>> rows = new ArrayList<>();
        Map<String, AmzaSyncStats.Stat> map = stats.ingressedMap();
        List<Map.Entry<String, AmzaSyncStats.Stat>> sortedEntries = new ArrayList<>(map.entrySet());

        Collections.sort(sortedEntries,
            (o1, o2) -> Long.compare(o2.getValue().count.longValue(), o1.getValue().count.longValue()));

        long grandTotal = 0;
        long mostRecentUpdateTimestamp = 0;
        long worstLatency = 0;
        for (Map.Entry<String, AmzaSyncStats.Stat> e : sortedEntries) {
            Map<String, String> status = new HashMap<>();
            String key = e.getKey();
            status.put("context", key);
            AmzaSyncStats.Stat value = e.getValue();
            status.put("count", String.valueOf(value.count.get()));
            status.put("recency", humanReadableUptime(System.currentTimeMillis() - value.timestamp.get()));
            status.put("latency", humanReadableLatency(value.latency.get()));
            rows.add(status);
            if (value.timestamp.get() > mostRecentUpdateTimestamp) {
                mostRecentUpdateTimestamp = value.timestamp.get();
            }
            if (value.latency.get() > worstLatency) {
                worstLatency = value.latency.get();
            }
            grandTotal += value.count.get();
        }

        data.put("ingressedRecency", mostRecentUpdateTimestamp == 0 ? "" : humanReadableUptime(System.currentTimeMillis() - mostRecentUpdateTimestamp));
        data.put("ingressedLatency", humanReadableLatency(worstLatency));
        data.put("ingressedTotal", String.valueOf(grandTotal));
        data.put("ingressedStatus", rows);
    }

    private void egressed(Map<String, Object> data) {
        List<Map<String, String>> rows = new ArrayList<>();
        Map<String, AmzaSyncStats.Stat> map = stats.egressedMap();
        List<Map.Entry<String, AmzaSyncStats.Stat>> sortedEntries = new ArrayList<>(map.entrySet());

        Collections.sort(sortedEntries,
            (o1, o2) -> Long.compare(o2.getValue().count.longValue(), o1.getValue().count.longValue()));

        long grandTotal = 0;
        long mostRecentUpdateTimestamp = 0;
        long worstLatency = 0;
        for (Map.Entry<String, AmzaSyncStats.Stat> e : sortedEntries) {
            Map<String, String> status = new HashMap<>();
            String key = e.getKey();
            status.put("context", key);
            AmzaSyncStats.Stat value = e.getValue();
            status.put("count", String.valueOf(value.count.get()));
            status.put("recency", humanReadableUptime(System.currentTimeMillis() - value.timestamp.get()));
            status.put("latency", humanReadableLatency(value.latency.get()));
            rows.add(status);
            if (value.timestamp.get() > mostRecentUpdateTimestamp) {
                mostRecentUpdateTimestamp = value.timestamp.get();
            }
            if (value.latency.get() > worstLatency) {
                worstLatency = value.latency.get();
            }
            grandTotal += value.count.get();
        }

        data.put("egressedRecency", mostRecentUpdateTimestamp == 0 ? "" : humanReadableUptime(System.currentTimeMillis() - mostRecentUpdateTimestamp));
        data.put("egressedLatency", humanReadableLatency(worstLatency));
        data.put("egressedTotal", String.valueOf(grandTotal));
        data.put("egressedStatus", rows);
    }

    @Override
    public String getTitle() {
        return "Sync";
    }

    public static String humanReadableLatency(long millis) {
        if (millis < 0) {
            return String.valueOf(millis);
        }

        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        millis -= TimeUnit.SECONDS.toMillis(seconds);

        StringBuilder sb = new StringBuilder(64);
        sb.append(seconds);
        sb.append(".");

        if (millis < 100) {
            sb.append('0');
        }
        if (millis < 10) {
            sb.append('0');
        }
        sb.append(millis);
        return (sb.toString());
    }

    public static String humanReadableUptime(long millis) {
        if (millis < 0) {
            return String.valueOf(millis);
        }

        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        millis -= TimeUnit.SECONDS.toMillis(seconds);

        StringBuilder sb = new StringBuilder(64);
        if (hours < 10) {
            sb.append('0');
        }
        sb.append(hours);
        sb.append(":");
        if (minutes < 10) {
            sb.append('0');
        }
        sb.append(minutes);
        sb.append(":");
        if (seconds < 10) {
            sb.append('0');
        }
        sb.append(seconds);

        return (sb.toString());
    }
}
