package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.ui.region.AquariumPluginRegion.AquariumPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.lab.guts.LABSparseCircularMetricBuffer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.awt.Color;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 */
// soy.page.aquariumPluginRegion
public class AquariumPluginRegion implements PageRegion<AquariumPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingStoreReader ringReader;
    private final AmzaAquariumProvider aquariumProvider;
    private final Liveliness liveliness;
    private final AquariumStats aquariumStats;

    public final EnumMap<State, LABSparseCircularMetricBuffer> currentStateMetricBuffer = new EnumMap<>(State.class);
    public final EnumMap<State, LABSparseCircularMetricBuffer> desiredStateMetricBuffer = new EnumMap<>(State.class);

    public AquariumPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingStoreReader ringReader,
        AmzaAquariumProvider aquariumProvider,
        Liveliness liveliness,
        AquariumStats aquariumStats) {
        this.template = template;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.aquariumProvider = aquariumProvider;
        this.liveliness = liveliness;
        this.aquariumStats = aquariumStats;

        for (State state : State.values()) {
            currentStateMetricBuffer.put(state, new LABSparseCircularMetricBuffer(120, 0, 1_000));
            desiredStateMetricBuffer.put(state, new LABSparseCircularMetricBuffer(120, 0, 1_000));
        }

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                refresh();
            } catch (Exception x) {
                LOG.warn("Refresh aquarium stats failed", x);
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }

    public void refresh() {
        long timestamp = System.currentTimeMillis();
        for (Entry<State, LongAdder> e : aquariumStats.currentState.entrySet()) {
            currentStateMetricBuffer.get(e.getKey()).set(timestamp, e.getValue());
        }
        for (Entry<State, LongAdder> e : aquariumStats.desiredState.entrySet()) {
            desiredStateMetricBuffer.get(e.getKey()).set(timestamp, e.getValue());
        }
    }

    public static class AquariumPluginRegionInput {

        final String ringName;
        final String partitionName;
        final String hexPartitionVersion;

        public AquariumPluginRegionInput(String ringName, String partitionName, String hexPartitionVersion) {
            this.ringName = ringName;
            this.partitionName = partitionName;
            this.hexPartitionVersion = hexPartitionVersion;
        }

    }

    @Override
    public String render(AquariumPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();
        try {
            data.put("ringName", input.ringName);
            data.put("partitionName", input.partitionName);
            data.put("partitionVersion", input.hexPartitionVersion);

            List<Map<String, String>> currentStats = Lists.newArrayList();
            currentStats.add(ImmutableMap.of("name", "getMyCurrentWaterline", "value", numberFormat.format(aquariumStats.getMyCurrentWaterline.longValue())));
            currentStats.add(ImmutableMap.of("name", "getOthersCurrentWaterline", "value", numberFormat.format(aquariumStats.getOthersCurrentWaterline
                .longValue())));
            currentStats
                .add(ImmutableMap.of("name", "acknowledgeCurrentOther", "value", numberFormat.format(aquariumStats.acknowledgeCurrentOther.longValue())));

            List<Map<String, String>> desiredStats = Lists.newArrayList();
            desiredStats.add(ImmutableMap.of("name", "getMyDesiredWaterline", "value", numberFormat.format(aquariumStats.getMyDesiredWaterline.longValue())));
            desiredStats.add(ImmutableMap.of("name", "getOthersDesiredWaterline", "value", numberFormat.format(aquariumStats.getOthersDesiredWaterline
                .longValue())));
            desiredStats
                .add(ImmutableMap.of("name", "acknowledgeDesiredOther", "value", numberFormat.format(aquariumStats.acknowledgeDesiredOther.longValue())));

            Map<String, Object> stats = Maps.newHashMap();
            stats.put("tapTheGlass", numberFormat.format(aquariumStats.tapTheGlass.longValue()));
            stats.put("feedTheFish", numberFormat.format(aquariumStats.feedTheFish.longValue()));
            stats.put("acknowledgeOther", numberFormat.format(aquariumStats.acknowledgeOther.longValue()));

            stats.put("awaitOnline", numberFormat.format(aquariumStats.awaitOnline.longValue()));
            stats.put("awaitTimedOut", numberFormat.format(aquariumStats.awaitTimedOut.longValue()));

            stats.put("current", currentStats);
            stats.put("desired", desiredStats);
            data.put("stats", stats);

            List<Map<String, Object>> wavformGroups = Lists.newArrayList();

            State[] aquariumStates = State.values();
            String[] names = new String[aquariumStates.length * 2];
            LABSparseCircularMetricBuffer[] waves = new LABSparseCircularMetricBuffer[aquariumStates.length * 2];
            boolean[] fills = new boolean[aquariumStates.length * 2];

            for (int i = 0; i < aquariumStates.length; i++) {
                int j = i * 2;
                fills[j] = false;
                fills[j + 1] = false;

                names[j] = "current-" + aquariumStates[i];
                names[j + 1] = "desired-" + aquariumStates[i];

                waves[j] = currentStateMetricBuffer.get(aquariumStates[i]);
                waves[j + 1] = desiredStateMetricBuffer.get(aquariumStates[i]);

            }

            wavformGroups.addAll(wavformGroup("states", null, "aquarium-states", stateColors, names, waves, fills));

            data.put("wavestats", wavformGroups);

            long now = System.currentTimeMillis();
            List<Map<String, Object>> live = new ArrayList<>();
            RingTopology ring = ringReader.getRing(AmzaRingReader.SYSTEM_RING);
            for (RingMemberAndHost entry : ring.entries) {
                long aliveUntilTimestamp = liveliness.aliveUntilTimestamp(entry.ringMember.asAquariumMember());

                live.add(ImmutableMap.of(
                    "member", entry.ringMember.getMember(),
                    "host", entry.ringHost.toCanonicalString(),
                    "liveliness", (aliveUntilTimestamp > now) ? "alive for " + String.valueOf(aliveUntilTimestamp - now) : "dead for " + String.valueOf(
                        aliveUntilTimestamp - now)
                ));
            }
            data.put("liveliness", live);

            byte[] ringNameBytes = input.ringName.getBytes();
            byte[] partitionNameBytes = input.partitionName.getBytes();
            PartitionName partitionName = (ringNameBytes.length > 0 && partitionNameBytes.length > 0)
                ? new PartitionName(false, ringNameBytes, partitionNameBytes) : null;
            long partitionVersion = Long.parseLong(input.hexPartitionVersion, 16);
            VersionedPartitionName versionedPartitionName = partitionName != null ? new VersionedPartitionName(partitionName, partitionVersion) : null;
            if (versionedPartitionName != null) {
                List<Map<String, Object>> states = new ArrayList<>();
                aquariumProvider.tx(versionedPartitionName, (readCurrent, readDesired, writeCurrent, writeDesired) -> {
                    for (RingMemberAndHost entry : ring.entries) {
                        Member asMember = entry.ringMember.asAquariumMember();

                        Map<String, Object> state = new HashMap<>();
                        state.put("partitionName", input.partitionName);
                        state.put("ringName", input.ringName);
                        state.put("partitionVersion", input.hexPartitionVersion);
                        if (readCurrent != null) {
                            Waterline current = readCurrent.get(asMember);
                            if (current != null) {
                                state.put("current", asMap(liveliness, current));
                            }
                        }
                        if (readDesired != null) {
                            Waterline desired = readDesired.get(asMember);
                            if (desired != null) {
                                state.put("desired", asMap(liveliness, desired));
                            }
                        }
                        states.add(state);
                    }
                    return true;
                });
                data.put("aquarium", states);
            }
        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    private Color[] stateColors = new Color[]{
        new Color(0, 0, 128),
        Color.blue,
        new Color(128, 0, 0),
        Color.red,
        new Color(128, 100, 0),
        Color.orange,
        new Color(0, 128, 0),
        Color.green,
        new Color(0, 64, 64), // teal
        new Color(0, 128, 128), // teal
        new Color(128, 0, 128),
        Color.magenta,
        Color.darkGray,
        Color.gray
    };

    private static Map<String, Object> asMap(Liveliness liveliness, Waterline waterline) throws Exception {
        State state = waterline.getState();
        Map<String, Object> map = new HashMap<>();
        map.put("state", state == null ? "null" : state.name());
        map.put("member", new String(waterline.getMember().getMember()));
        map.put("timestamp", String.valueOf(waterline.getTimestamp()));
        map.put("version", String.valueOf(waterline.getVersion()));
        map.put("alive", String.valueOf(liveliness.isAlive(waterline.getMember())));
        map.put("quorum", waterline.isAtQuorum());
        return map;
    }

    private List<Map<String, Object>> wavformGroup(String group, String filter, String title, Color[] colors, String[] waveName,
        LABSparseCircularMetricBuffer[] waveforms,
        boolean[] fill) {
        if (filter != null && filter.length() > 0 && !title.contains(filter)) {
            return Collections.emptyList();
        }

        String total = "";
        List<String> ls = new ArrayList<>();
        List<Map<String, Object>> ws = new ArrayList<>();
        int s = 1;
        for (double m : waveforms[0].metric()) {
            ls.add("\"" + s + "\"");
            s++;
        }

        for (int i = 0; i < waveName.length; i++) {
            List<String> values = Lists.newArrayList();
            double[] metric = waveforms[i].metric();
            for (double m : metric) {
                values.add("\"" + String.valueOf(m) + "\"");
            }
            ws.add(waveform(waveName[i], new Color[]{colors[i]}, 1f, values, fill[i], false));
            if (i > 0) {
                total += ", ";
            }
            Color c = colors[i];
            int r = c.getRed();
            int g = c.getGreen();
            int b = c.getBlue();
            String colorDiv = "<div style=\"display:inline-block; width:10px; height:10px; background:rgb(" + r + "," + g + "," + b + ");\"></div>";

            total += colorDiv + waveName[i] + "=" + numberFormat.format(waveforms[i].total());
        }

        List<Map<String, Object>> listOfwaveformGroups = Lists.newArrayList();

        List<Map<String, Object>> ows = new ArrayList<>();
        List<String> ols = new ArrayList<>();
        List<String> ovalues = Lists.newArrayList();
        Color[] ocolors = new Color[waveforms.length];
        for (int i = 0; i < waveforms.length; i++) {
            ovalues.add("\"" + String.valueOf(waveforms[i].total()) + "\"");
            ols.add("\"" + waveName[i] + "\"");
            ocolors[i] = colors[i];
        }
        ows.add(waveform(title + "-overview", ocolors, 1f, ovalues, true, false));

        Map<String, Object> overViewMap = new HashMap<>();
        overViewMap.put("group", group);
        overViewMap.put("filter", title);
        overViewMap.put("title", title + "-overview");
        overViewMap.put("total", "");
        overViewMap.put("height", String.valueOf(150));
        overViewMap.put("width", String.valueOf(ls.size() * 10));
        overViewMap.put("id", title + "-overview");
        overViewMap.put("graphType", "bar");
        overViewMap.put("waveform", ImmutableMap.of("labels", ols, "datasets", ows));
        listOfwaveformGroups.add(overViewMap);

        Map<String, Object> map = new HashMap<>();
        map.put("group", group);
        map.put("filter", title);
        map.put("title", title);
        map.put("total", total);

        if (filter != null && filter.length() > 0) {
            map.put("height", String.valueOf(800));
        } else {
            map.put("height", String.valueOf(300));
        }
        map.put("width", String.valueOf(ls.size() * 10));
        map.put("id", title);
        map.put("graphType", "line");
        map.put("waveform", ImmutableMap.of("labels", ls, "datasets", ws));
        listOfwaveformGroups.add(map);
        return listOfwaveformGroups;
    }

    public Map<String, Object> waveform(String label, Color[] color, float alpha, List<String> values, boolean fill, boolean stepped) {
        Map<String, Object> waveform = new HashMap<>();
        waveform.put("label", "\"" + label + "\"");

        Object c = "\"rgba(" + color[0].getRed() + "," + color[0].getGreen() + "," + color[0].getBlue() + "," + String.valueOf(alpha) + ")\"";
        if (color.length > 1) {
            List<String> colorStrings = Lists.newArrayList();
            for (int i = 0; i < color.length; i++) {
                colorStrings.add("\"rgba(" + color[i].getRed() + "," + color[i].getGreen() + "," + color[i].getBlue() + "," + String.valueOf(alpha) + ")\"");
            }
            c = colorStrings;
        }

        waveform.put("fill", fill);
        waveform.put("steppedLine", stepped);
        waveform.put("lineTension", "0.1");
        waveform.put("backgroundColor", c);
        waveform.put("borderColor", c);
        waveform.put("borderCapStyle", "'butt'");
        waveform.put("borderDash", "[]");
        waveform.put("borderDashOffset", 0.0);
        waveform.put("borderJoinStyle", "'miter'");
        waveform.put("pointBorderColor", c);
        waveform.put("pointBackgroundColor", "\"#fff\"");
        waveform.put("pointBorderWidth", 1);
        waveform.put("pointHoverRadius", 5);
        waveform.put("pointHoverBackgroundColor", c);
        waveform.put("pointHoverBorderColor", c);
        waveform.put("pointHoverBorderWidth", 2);
        waveform.put("pointRadius", 1);
        waveform.put("pointHitRadius", 10);
        waveform.put("spanGaps", false);

        waveform.put("data", values);
        return waveform;
    }

    @Override
    public String getTitle() {
        return "Aquarium";
    }

}
