package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.ImmutableMap;
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
import com.jivesoftware.os.aquarium.Liveliness;
import com.jivesoftware.os.aquarium.Member;
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
