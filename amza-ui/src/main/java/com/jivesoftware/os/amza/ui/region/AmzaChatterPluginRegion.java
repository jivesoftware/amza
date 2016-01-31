package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.VersionedAquarium;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.Partition;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.take.TakeCoordinator;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.aquarium.LivelyEndState;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.Waterline;
import com.jivesoftware.os.jive.utils.ordered.id.IdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.getDurationBreakdown;

/**
 *
 * @author jonathan.colt
 */
public class AmzaChatterPluginRegion implements PageRegion<AmzaChatterPluginRegion.ChatterPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaService amzaService;
    private final AmzaStats amzaStats;
    private final TimestampProvider timestampProvider;
    private final IdPacker idPacker;

    public AmzaChatterPluginRegion(String template,
        SoyRenderer renderer,
        AmzaService amzaService,
        AmzaStats amzaStats,
        TimestampProvider timestampProvider,
        IdPacker idPacker) {
        this.template = template;
        this.renderer = renderer;
        this.amzaService = amzaService;
        this.amzaStats = amzaStats;
        this.timestampProvider = timestampProvider;
        this.idPacker = idPacker;

    }

    @Override
    public String getTitle() {
        return "Chatter";
    }

    public static class ChatterPluginRegionInput {

        public ChatterPluginRegionInput() {

        }
    }

    @Override
    public String render(AmzaChatterPluginRegion.ChatterPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            TreeMap<RingMember, RingHost> nodes = new TreeMap<>();
            amzaService.getRingReader().streamRingMembersAndHosts((ringMemberAndHost) -> {
                nodes.put(ringMemberAndHost.ringMember, ringMemberAndHost.ringHost);
                return true;
            });

            List<Object> header = new ArrayList<>();
            header.add(Collections.emptyList());
            header.add(Collections.emptyList());
            header.add(Collections.emptyList());
            header.add(Collections.emptyList());

            int partitionNameIndex = 0;
            int partitionInteractionIndex = 1;
            int partitionStatsIndex = 2;
            int electionIndex = 3;

            int columnIndex = 4;
            Map<RingMember, Integer> ringMemeberToColumIndex = new HashMap<>();
            for (RingMember ringMember : nodes.keySet()) {

                RingHost host = nodes.get(ringMember);
                header.add(Arrays.asList(
                    new Element("id", "id", null, ringMember.getMember(), "primary"),
                    new Element("host", "host", null, host.getHost(), "primary")
                ));

                ringMemeberToColumIndex.put(ringMember, columnIndex);
                columnIndex++;
            }

            data.put("header", header);
            List<Object> rows = new ArrayList<>();

            int totalColumns = columnIndex;

            long wallTimeMillis = System.currentTimeMillis();
            long currentTime = timestampProvider.getApproximateTimestamp(wallTimeMillis);

            TakeCoordinator takeCoordinator = amzaService.getTakeCoordinator();
            takeCoordinator.streamCategories((versionedPartitionName, category) -> {
                Element[][] cells = new Element[totalColumns][];

                Partition partition = amzaService.getPartition(versionedPartitionName.getPartitionName());
                LivelyEndState[] livelyEndState = new LivelyEndState[1];
                partition.highestTxId((VersionedAquarium versionedAquarium, long highestTxId) -> {
                    livelyEndState[0] = versionedAquarium.getLivelyEndState();
                    return highestTxId;
                });

                cells[partitionNameIndex] = new Element[]{
                    new Element("ring", "ring", new String(versionedPartitionName.getPartitionName().getRingName(), StandardCharsets.UTF_8), null, "info"),
                    new Element("paritition", "paritition", new String(versionedPartitionName.getPartitionName().getName(), StandardCharsets.UTF_8), null,
                    "info"),
                    new Element("category", "category", null, String.valueOf(category), "info")
                };

                AmzaStats.Totals totals = amzaStats.getPartitionTotals().get(versionedPartitionName.getPartitionName());
                if (totals != null) {
                    cells[partitionInteractionIndex] = new Element[]{
                        new Element("interactions", null, "gets", numberFormat.format(totals.gets.get()), null),
                        new Element("interactions", null, "getsLag", getDurationBreakdown(totals.getsLag.get()), null),
                        new Element("interactions", null, "scans", numberFormat.format(totals.scans.get()), null),
                        new Element("interactions", null, "scansLag", getDurationBreakdown(totals.scansLag.get()), null),
                        new Element("interactions", null, "directApplies", numberFormat.format(totals.directApplies.get()), null),
                        new Element("interactions", null, "directAppliesLag", getDurationBreakdown(totals.directAppliesLag.get()), null),
                        new Element("interactions", null, "updates", numberFormat.format(totals.updates.get()), null),
                        new Element("interactions", null, "updatesLag", getDurationBreakdown(totals.updatesLag.get()), null),
                    };

                    cells[partitionStatsIndex] = new Element[]{
                        new Element("stat", null, "offers", numberFormat.format(totals.offers.get()), null),
                        new Element("stat", null, "offersLag", getDurationBreakdown(totals.offersLag.get()), null),
                        new Element("stat", null, "takes", numberFormat.format(totals.takes.get()), null),
                        new Element("stat", null, "takesLag", getDurationBreakdown(totals.takesLag.get()), null),
                        new Element("stat", null, "takeApplies", numberFormat.format(totals.takeApplies.get()), null),
                        new Element("stat", null, "takeAppliesLag", getDurationBreakdown(totals.takeAppliesLag.get()), null)
                    };
                }

                Waterline currentWaterline = livelyEndState[0] != null ? livelyEndState[0].getCurrentWaterline() : null;
                if (currentWaterline != null) {

                    State state = currentWaterline.getState();
                    cells[electionIndex] = new Element[]{
                        new Element("election", state.name(), null, state.name(),
                        (state == State.leader || state == State.follower) ? "success" : "warning"),
                        new Element("online", "online", "online", String.valueOf(livelyEndState[0].isOnline()),
                        livelyEndState[0].isOnline() ? "success" : "warning"),
                        new Element("quorum", "quorum", "quorum", String.valueOf(currentWaterline.isAtQuorum()),
                        currentWaterline.isAtQuorum() ? "success" : "warning"
                        )
                    };
                } else {
                    cells[electionIndex] = new Element[]{
                        new Element("election", "election", null, "unknown", "danger")
                    };
                }

                takeCoordinator.streamTookLatencies(versionedPartitionName,
                    (ringMember, lastOfferedTxId, category1, tooSlowTxId, takeSessionId, online, steadyState, lastOfferedMillis,
                        lastTakenMillis, lastCategoryCheckMillis) -> {

                        Integer index = ringMemeberToColumIndex.get(ringMember);
                        if (index != null) {

                            long offeredLatency = currentTime - lastOfferedMillis;
                            if (offeredLatency < 0) {
                                offeredLatency = 0;
                            }

                            long takenLatency = currentTime - lastTakenMillis;
                            if (takenLatency < 0) {
                                takenLatency = 0;
                            }

                            long categoryLatency = currentTime - lastCategoryCheckMillis;
                            if (categoryLatency < 0) {
                                categoryLatency = 0;
                            }

                            if (lastOfferedTxId != -1) {
                                long lastOfferedTimestamp = idPacker.unpack(lastOfferedTxId)[0];
                                long tooSlowTimestamp = idPacker.unpack(tooSlowTxId)[0];
                                long latencyInMillis = currentTime - lastOfferedTimestamp;

                                cells[index] = new Element[]{
                                    new Element("session", "session", "session", String.valueOf(takeSessionId), null),
                                    new Element("online", "online", "online", String.valueOf(online),
                                        online ? "success" : "warning"),
                                    new Element("quorum", "quorum", "quorum", String.valueOf(currentWaterline.isAtQuorum()),
                                        currentWaterline.isAtQuorum() ? "success" : "warning"),
                                    new Element("category", "category", "category", String.valueOf(category1),
                                        category1 == 1 ? "success" : "warning"),
                                    new Element("latency", "latency", "latency", ((latencyInMillis < 0) ? '-' : ' ') + getDurationBreakdown(Math.abs(
                                        latencyInMillis)), null),
                                    new Element("slow", "latency", "slow", getDurationBreakdown(tooSlowTimestamp), null),
                                    new Element("steadyState", "steadyState", "steadyState", String.valueOf(steadyState),
                                        steadyState ? "default" : "success"),
                                    new Element("offered", "offered", "offered", lastOfferedMillis == -1 ? "unknown" : getDurationBreakdown(offeredLatency),
                                        lastOfferedMillis == -1 ? "danger " : null
                                    ),
                                    new Element("taken", "taken", "taken", lastTakenMillis == -1 ? "unknown" : getDurationBreakdown(takenLatency),
                                        lastTakenMillis == -1 ? "danger " : null
                                    ),
                                    new Element("category", "category", "category", lastCategoryCheckMillis == -1 ? "unknown" : getDurationBreakdown(
                                        categoryLatency),
                                        lastCategoryCheckMillis == -1 ? "danger " : null
                                    )
                                };
                            } else {

                                cells[electionIndex] = new Element[]{
                                    new Element("session", "session", "session", String.valueOf(takeSessionId), null),
                                    new Element("online", "online", "online", String.valueOf(online),
                                        online ? "success" : "warning"),
                                    new Element("quorum", "quorum", "quorum", String.valueOf(currentWaterline.isAtQuorum()),
                                        currentWaterline.isAtQuorum() ? "success" : "warning"),
                                    new Element("category", "category", "category", String.valueOf(category1),
                                        category1 == 1 ? "success" : "warning"),
                                    new Element("latency", "latency", "latency", "never", "warning"),
                                    new Element("slow", "latency", "slow", "never", "warning"),
                                    new Element("steadyState", "steadyState", "steadyState", String.valueOf(steadyState), steadyState ? "default" : "success"),
                                    new Element("offered", "offered", "offered", lastOfferedMillis == -1 ? "unknown" : getDurationBreakdown(offeredLatency),
                                        lastOfferedMillis == -1 ? "danger " : null
                                    ),
                                    new Element("taken", "taken", "taken", lastTakenMillis == -1 ? "unknown" : getDurationBreakdown(takenLatency),
                                        lastTakenMillis == -1 ? "danger " : null
                                    ),
                                    new Element("category", "category", "category", lastCategoryCheckMillis == -1 ? "unknown" : getDurationBreakdown(
                                        categoryLatency),
                                        lastCategoryCheckMillis == -1 ? "danger " : null
                                    )
                                };
                            }
                        } else {
                            cells[index] = new Element[]{
                                new Element("id", "id", null, ringMember.getMember(), "danger"),};
                        }
                        return true;
                    });

                List<Object> row = new ArrayList<>();
                for (Element[] elements : cells) {
                    if (elements == null) {
                        row.add(Collections.emptyList());
                    } else {
                        row.add(Arrays.asList(elements));
                    }
                }
                rows.add(row);

                return true;

            });

            data.put("rows", rows);

            return renderer.render(template, data);
        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
            return "Error";
        }

    }

    static class Element extends HashMap<String, String> {

        public Element(String type, String icon, String name, String value, String mode) {
            put("type", "unkown");
            if (icon != null) {
                put("icon", icon);
            }
            if (name != null) {
                put("name", name);
            }
            if (value != null) {
                put("value", value);
            }
            if (mode != null) {
                put("mode", mode);
            }
        }

    }
}
