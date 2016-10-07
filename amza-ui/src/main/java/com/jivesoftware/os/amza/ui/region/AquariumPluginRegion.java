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
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 */
// soy.page.aquariumPluginRegion
public class AquariumPluginRegion implements PageRegion<AquariumPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final NumberFormat format = NumberFormat.getInstance();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingStoreReader ringReader;
    private final AmzaAquariumProvider aquariumProvider;
    private final Liveliness liveliness;
    private final AquariumStats aquariumStats;

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
            currentStats.add(ImmutableMap.of("name", "getMyCurrentWaterline", "value", format.format(aquariumStats.getMyCurrentWaterline.longValue())));
            currentStats.add(ImmutableMap.of("name", "getOthersCurrentWaterline", "value", format.format(aquariumStats.getOthersCurrentWaterline.longValue())));
            currentStats.add(ImmutableMap.of("name", "acknowledgeCurrentOther", "value", format.format(aquariumStats.acknowledgeCurrentOther.longValue())));
            for (Map.Entry<State, LongAdder> entry : aquariumStats.currentState.entrySet()) {
                currentStats.add(ImmutableMap.of("name", "current-" + entry.getKey().name(), "value", format.format(entry.getValue().longValue())));
            }

            List<Map<String, String>> desiredStats = Lists.newArrayList();
            desiredStats.add(ImmutableMap.of("name", "getMyDesiredWaterline", "value", format.format(aquariumStats.getMyDesiredWaterline.longValue())));
            desiredStats.add(ImmutableMap.of("name", "getOthersDesiredWaterline", "value", format.format(aquariumStats.getOthersDesiredWaterline.longValue())));
            desiredStats.add(ImmutableMap.of("name", "acknowledgeDesiredOther", "value", format.format(aquariumStats.acknowledgeDesiredOther.longValue())));
            for (Map.Entry<State, LongAdder> entry : aquariumStats.currentState.entrySet()) {
                desiredStats.add(ImmutableMap.of("name", "desired-" + entry.getKey().name(), "value", format.format(entry.getValue().longValue())));
            }

            Map<String, Object> stats = Maps.newHashMap();
            stats.put("tapTheGlass", format.format(aquariumStats.tapTheGlass.longValue()));
            stats.put("feedTheFish", format.format(aquariumStats.feedTheFish.longValue()));
            stats.put("acknowledgeOther", format.format(aquariumStats.acknowledgeOther.longValue()));
            
            stats.put("awaitOnline", format.format(aquariumStats.awaitOnline.longValue()));
            stats.put("awaitTimedOut", format.format(aquariumStats.awaitTimedOut.longValue()));
            
            stats.put("current", currentStats);
            stats.put("desired", desiredStats);
            data.put("stats", stats);

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
            log.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

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

    @Override
    public String getTitle() {
        return "Aquarium";
    }

}
