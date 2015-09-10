package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.AmzaRingStoreReader;
import com.jivesoftware.os.amza.service.replication.AmzaAquariumProvider;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.ui.region.AquariumPluginRegion.AquariumPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.aquarium.Aquarium;
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.ReadWaterline;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.page.aquariumPluginRegion
public class AquariumPluginRegion implements PageRegion<AquariumPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingStoreReader ringReader;
    private final AmzaAquariumProvider aquariumProvider;
    private final Liveliness liveliness;

    public AquariumPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingStoreReader ringReader,
        AmzaAquariumProvider aquariumProvider,
        Liveliness liveliness) {
        this.template = template;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.aquariumProvider = aquariumProvider;
        this.liveliness = liveliness;
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

            long now = System.currentTimeMillis();
            List<Map<String, Object>> live = new ArrayList<>();
            for (Map.Entry<RingMember, RingHost> e : ringReader.getRing(AmzaRingReader.SYSTEM_RING).entrySet()) {
                long aliveUntilTimestamp;
                if (e.getKey().equals(ringReader.getRingMember())) {
                    aliveUntilTimestamp = liveliness.aliveUntilTimestamp();
                } else {
                    aliveUntilTimestamp = liveliness.otherAliveUntilTimestamp(e.getKey().asAquariumMember());
                }

                live.add(ImmutableMap.of(
                    "member", e.getKey().getMember(),
                    "host", e.getValue().toCanonicalString(),
                    "liveliness", (aliveUntilTimestamp > now) ? "alive for " + String.valueOf(aliveUntilTimestamp - now) : "dead for " + String.valueOf(
                            aliveUntilTimestamp - now)
                ));
            }
            data.put("liveliness", live);

            PartitionName partitionName = new PartitionName(false, input.ringName.getBytes(), input.partitionName.getBytes());
            long partitionVersion = Long.parseLong(input.hexPartitionVersion, 16);
            VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, partitionVersion);
            Aquarium aquarium = aquariumProvider.getAquarium(versionedPartitionName);
            if (aquarium != null) {
                List<Map<String, Object>> states = new ArrayList<>();
                for (Map.Entry<RingMember, RingHost> e : ringReader.getRing(AmzaRingReader.SYSTEM_RING).entrySet()) {
                    aquarium.inspectState(e.getKey().asAquariumMember(), (ReadWaterline readCurrent, ReadWaterline readDesired) -> {

                        Map<String, Object> state = new HashMap<>();
                        state.put("partitionName", input.partitionName);
                        state.put("ringName", input.ringName);
                        state.put("partitionVersion", input.hexPartitionVersion);
                        if (readCurrent != null) {
                            Waterline current = readCurrent.get();
                            if (current != null) {
                                if (current.getMember().equals(e.getKey().asAquariumMember())) {
                                    state.put("current", asMap(current, now));
                                }
                                List<Map<String, Object>> others = new ArrayList<>();
                                readCurrent.getOthers((Waterline waterline) -> {
                                    if (waterline.getMember().equals(e.getKey().asAquariumMember())) {
                                        others.add(asMap(waterline, now));
                                    }
                                    return true;
                                });
                                if (!others.isEmpty()) {
                                    state.put("othersCurrent", others);
                                }
                            }
                        }
                        if (readDesired != null) {
                            Waterline desired = readDesired.get();
                            if (desired != null) {
                                if (desired.getMember().equals(e.getKey().asAquariumMember())) {
                                    state.put("desired", asMap(desired, now));
                                }
                                List<Map<String, Object>> others = new ArrayList<>();
                                readDesired.getOthers((Waterline waterline) -> {
                                    if (waterline.getMember().equals(e.getKey().asAquariumMember())) {
                                        others.add(asMap(waterline, now));
                                    }
                                    return true;
                                });
                                if (!others.isEmpty()) {
                                    state.put("othersDesired", others);
                                }
                            }
                        }
                        states.add(state);
                        return true;
                    });
                }
                data.put("aquarium", states);
            }

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }
        return renderer.render(template, data);
    }

    private static Map<String, Object> asMap(Waterline waterline, long now) {
        State state = waterline.getState();
        Map<String, Object> map = new HashMap<>();
        map.put("state", state == null ? "null" : state.name());
        map.put("member", new String(waterline.getMember().getMember()));
        map.put("timestamp", String.valueOf(waterline.getTimestamp()));
        map.put("version", String.valueOf(waterline.getVersion()));
        map.put("alive", String.valueOf(waterline.isAlive(now)));
        map.put("quorum", waterline.isAtQuorum());
        return map;
    }

    @Override
    public String getTitle() {
        return "Aquarium";
    }

}
