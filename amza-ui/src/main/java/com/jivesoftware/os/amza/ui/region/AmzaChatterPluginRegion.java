package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.mutable.MutableLong;

import static com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.getDurationBreakdown;

/**
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

        public final boolean unhealthy;
        public final boolean active;
        public final boolean system;

        public ChatterPluginRegionInput(boolean unhealthy, boolean active, boolean system) {
            this.unhealthy = unhealthy;
            this.active = active;
            this.system = system;
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

            Multiset<String> stateCounts = HashMultiset.<String>create();

            List<List> header = new ArrayList<>();
            header.add(new ArrayList<>());
            header.add(new ArrayList<>());
            header.add(new ArrayList<>());
            header.add(new ArrayList<>());

            int partitionNameIndex = 0;
            int partitionInteractionIndex = 1;
            int partitionStatsIndex = 2;
            int electionIndex = 3;

            int columnIndex = 4;
            Map<RingMember, Integer> ringMemberToColumnIndex = new HashMap<>();
            for (RingMember ringMember : nodes.keySet()) {

                RingHost host = nodes.get(ringMember);
                header.add(Arrays.asList(
                    new Element("id", "id", null, ringMember.getMember(), null),
                    new Element("host", "host", null, host.getHost(), null)
                ));

                ringMemberToColumnIndex.put(ringMember, columnIndex);
                columnIndex++;
            }

            data.put("header", header);
            List<Object> unhealthyRows = new ArrayList<>();
            List<Object> activeRows = new ArrayList<>();
            List<Object> systemRows = new ArrayList<>();

            MutableLong unhealthyCount = new MutableLong();
            MutableLong activeCount = new MutableLong();
            MutableLong systemCount = new MutableLong();

            int totalColumns = columnIndex;

            long wallTimeMillis = System.currentTimeMillis();
            long currentTime = timestampProvider.getApproximateTimestamp(wallTimeMillis);

            TakeCoordinator takeCoordinator = amzaService.getTakeCoordinator();
            takeCoordinator.streamCategories((versionedPartitionName, category, ringCallCount, partitionCallCount) -> {

                AtomicBoolean unhealthy = new AtomicBoolean(false);
                AtomicBoolean active = new AtomicBoolean(false);

                Element[][] cells = new Element[totalColumns][];

                Partition partition = amzaService.getPartition(versionedPartitionName.getPartitionName());
                LivelyEndState[] livelyEndState = new LivelyEndState[1];
                livelyEndState[0] = partition.livelyEndState();

                cells[partitionNameIndex] = new Element[]{
                    new Element("ring", "ring", new String(versionedPartitionName.getPartitionName().getRingName(), StandardCharsets.UTF_8), null,
                    null),
                    new Element("partition", "partition", new String(versionedPartitionName.getPartitionName().getName(), StandardCharsets.UTF_8), null,
                    null),
                    new Element("category", "category", null, String.valueOf(category),
                    null)
                };

                AmzaStats.Totals totals = amzaStats.getPartitionTotals().get(versionedPartitionName.getPartitionName());
                if (totals != null) {
                    cells[partitionInteractionIndex] = new Element[]{
                        totals.gets.get() < 1 ? null : new Element("interactions", "interactions", "gets", numberFormat.format(totals.gets.get()), null),
                        totals.getsLatency.get() < 1 ? null : new Element("interactions", "interactions", "getsLag", getDurationBreakdown(totals.getsLatency
                        .get()),
                        null),
                        totals.scans.get() < 1 ? null : new Element("interactions", "interactions", "scans", numberFormat.format(totals.scans.get()), null),
                        totals.scansLatency.get() < 1 ? null : new Element("interactions", "interactions", "scansLag", getDurationBreakdown(totals.scansLatency
                        .get()),
                        null),
                        totals.directApplies.get() < 1 ? null : new Element("interactions", "interactions", "directApplies", numberFormat.format(
                        totals.directApplies.get()), null),
                        totals.directAppliesLag.get() < 1 ? null : new Element("interactions", "interactions", "directAppliesLag", getDurationBreakdown(
                        totals.directAppliesLag.get()), null),
                        totals.updates.get() < 1 ? null : new Element("interactions", "interactions", "updates", numberFormat.format(totals.updates.get()),
                        null),
                        totals.updatesLag.get() < 1 ? null : new Element("interactions", "interactions", "updatesLag", getDurationBreakdown(totals.updatesLag
                        .get()), null)};

                    cells[partitionStatsIndex] = new Element[]{
                        totals.offers.get() < 1 ? null : new Element("stats", "stats", "offers", numberFormat.format(totals.offers.get()), null),
                        totals.offersLag.get() < 1 ? null : new Element("stats", "stats", "offersLag", getDurationBreakdown(totals.offersLag.get()), null),
                        totals.takes.get() < 1 ? null : new Element("stats", "stats", "takes", numberFormat.format(totals.takes.get()), null),
                        totals.takesLag.get() < 1 ? null : new Element("stats", "stats", "takesLag", getDurationBreakdown(totals.takesLag.get()), null),
                        totals.takeApplies.get() < 1 ? null : new Element("stats", "stats", "takeApplies", numberFormat.format(totals.takeApplies.get()), null),
                        totals.takeAppliesLag.get() < 1 ? null : new Element("stats", "stats", "takeAppliesLag", getDurationBreakdown(totals.takeAppliesLag
                        .get()), null)
                    };
                }

                Waterline currentWaterline = livelyEndState[0] != null ? livelyEndState[0].getCurrentWaterline() : null;
                if (currentWaterline != null) {
                    boolean livelyOnline = livelyEndState[0].isOnline();
                    boolean atQuorum = currentWaterline.isAtQuorum();

                    State state = currentWaterline.getState();
                    unhealthy.compareAndSet(false, !(state == State.leader || state == State.follower));
                    unhealthy.compareAndSet(false, !livelyOnline);
                    unhealthy.compareAndSet(false, ringCallCount <= 0);
                    unhealthy.compareAndSet(false, partitionCallCount <= 0);

                    stateCounts.add(state.name());

                    cells[electionIndex] = new Element[]{
                        new Element("election", "election", state.name(), null,
                        (state == State.leader || state == State.follower) ? null : "warning"),
                        new Element("online", "online", "online", String.valueOf(livelyOnline),
                        livelyOnline ? null : "warning"),
                        new Element("quorum", "quorum", "quorum", String.valueOf(atQuorum),
                        atQuorum ? null : "warning"),
                        new Element("ring", "ring", "ring", String.valueOf(ringCallCount),
                        ringCallCount > 0 ? null : "warning"),
                        new Element("partition", "partition", "partition", String.valueOf(partitionCallCount),
                        partitionCallCount > 0 ? null : "warning")
                    };

                } else {
                    stateCounts.add("unknown");

                    cells[electionIndex] = new Element[]{
                        new Element("election", "election", null, "unknown", "danger")
                    };
                    unhealthy.compareAndSet(false, true);
                }

                takeCoordinator.streamTookLatencies(versionedPartitionName,
                    (ringMember, lastOfferedTxId, category1, tooSlowTxId, takeSessionId, online, steadyState, lastOfferedMillis,
                        lastTakenMillis, lastCategoryCheckMillis) -> {

                        Integer index = ringMemberToColumnIndex.get(ringMember);
                        if (index != null) {

                            long offeredLatency = wallTimeMillis - lastOfferedMillis;
                            if (offeredLatency < 0) {
                                offeredLatency = 0;
                            }

                            long takenLatency = wallTimeMillis - lastTakenMillis;
                            if (takenLatency < 0) {
                                takenLatency = 0;
                            }

                            long categoryLatency = wallTimeMillis - lastCategoryCheckMillis;
                            if (categoryLatency < 0) {
                                categoryLatency = 0;
                            }

                            boolean currentlyAtQuorum = currentWaterline != null && currentWaterline.isAtQuorum();

                            unhealthy.compareAndSet(false, !online);
                            unhealthy.compareAndSet(false, !currentlyAtQuorum);
                            unhealthy.compareAndSet(false, lastOfferedMillis == -1);
                            unhealthy.compareAndSet(false, lastTakenMillis == -1);
                            unhealthy.compareAndSet(false, lastCategoryCheckMillis == -1);

                            active.compareAndSet(false, !steadyState);

                            long tooSlowTimestamp = -1;
                            long latencyInMillis = -1;
                            if (lastOfferedTxId != -1) {
                                long lastOfferedTimestamp = idPacker.unpack(lastOfferedTxId)[0];
                                tooSlowTimestamp = idPacker.unpack(tooSlowTxId)[0];
                                latencyInMillis = currentTime - lastOfferedTimestamp;
                            }
                            String latency = ((latencyInMillis < 0) ? '-' : ' ') + getDurationBreakdown(Math.abs(latencyInMillis));
                            String tooSlow = getDurationBreakdown(tooSlowTimestamp);
                            cells[index] = new Element[]{
                                new Element("session", "session", "session",
                                    String.valueOf(takeSessionId),
                                    null),
                                new Element("online", "online", "online",
                                    String.valueOf(online),
                                    online ? null : "warning"),
                                new Element("quorum", "quorum", "quorum",
                                    String.valueOf(currentlyAtQuorum),
                                    currentlyAtQuorum ? null : "warning"),
                                new Element("category", "category", "category",
                                    String.valueOf(category1),
                                    category1 == 1 ? null : "warning"),
                                new Element("latency", "latency", "latency",
                                    lastOfferedTxId == -1 ? "never" : latency,
                                    null),
                                new Element("slow", "latency", "slow",
                                    lastOfferedTxId == -1 ? "never" : tooSlow,
                                    null),
                                new Element("steadyState", "steadyState", "steadyState",
                                    String.valueOf(steadyState),
                                    steadyState ? "danger" : null),
                                new Element("offered", "offered", "offered",
                                    lastOfferedMillis == -1 ? "unknown" : getDurationBreakdown(offeredLatency),
                                    lastOfferedMillis == -1 ? "danger " : null),
                                new Element("taken", "taken", "taken",
                                    lastTakenMillis == -1 ? "unknown" : getDurationBreakdown(takenLatency),
                                    lastTakenMillis == -1 ? "danger " : null),
                                new Element("category", "category", "category",
                                    lastCategoryCheckMillis == -1 ? "unknown" : getDurationBreakdown(categoryLatency),
                                    lastCategoryCheckMillis == -1 ? "danger " : null)
                            };

                        } else {

                            unhealthy.compareAndSet(false, true);
                            cells[index] = new Element[]{
                                new Element("id", "id", null, ringMember.getMember(), "danger")
                            };
                        }
                        return true;
                    });

                List<Object> row = new ArrayList<>();
                for (Element[] elements : cells) {
                    if (elements == null) {
                        row.add(Collections.emptyList());
                    } else {
                        List<Object> es = new ArrayList<>();
                        for (Element element : elements) {
                            if (element != null) {
                                es.add(element);
                            }
                        }
                        row.add(es);
                    }
                }
                if (unhealthy.get()) {
                    unhealthyCount.add(1);
                    if (input.unhealthy) {
                        unhealthyRows.add(row);
                    }
                } else if (active.get()) {
                    if (versionedPartitionName.getPartitionName().isSystemPartition()) {
                        systemCount.add(1);
                    } else {
                        activeCount.add(1);
                    }
                    if (versionedPartitionName.getPartitionName().isSystemPartition()) {
                        if (input.system) {
                            systemRows.add(row);
                        }
                    } else if (input.active) {
                        activeRows.add(row);
                    }
                }
                return true;
            });

            AmzaStats.Totals totals = amzaStats.getGrandTotal();
            if (totals != null) {
                List headerCell = ((List) header.get(partitionInteractionIndex));
                if (totals.gets.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "gets", numberFormat.format(totals.gets.get()), null));
                }
                if (totals.getsLatency.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "getsLatency", getDurationBreakdown(totals.getsLatency.get()), null));
                }
                if (totals.scans.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "scans", numberFormat.format(totals.scans.get()), null));
                }
                if (totals.scansLatency.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "scansLatency", getDurationBreakdown(totals.scansLatency.get()), null));
                }
                if (totals.directApplies.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "directApplies", numberFormat.format(totals.directApplies.get()), null));
                }
                if (totals.directAppliesLag.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "directAppliesLag", getDurationBreakdown(totals.directAppliesLag.get()), null));
                }
                if (totals.updates.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "updates", numberFormat.format(totals.updates.get()), null));
                }
                if (totals.updatesLag.get() > 0) {
                    headerCell.add(new Element("interactions", "interactions", "updatesLag", getDurationBreakdown(totals.updatesLag.get()), null));
                }

                headerCell = ((List) header.get(partitionStatsIndex));
                if (totals.offers.get() > 0) {
                    headerCell.add(new Element("stats", "stats", "offers", numberFormat.format(totals.offers.get()), null));
                }

                if (totals.offersLag.get() > 0) {
                    headerCell.add(new Element("stats", "stats", "offersLag", getDurationBreakdown(totals.offersLag.get()), null));
                }
                if (totals.takes.get() > 0) {
                    headerCell.add(new Element("stats", "stats", "takes", numberFormat.format(totals.takes.get()), null));
                }
                if (totals.takesLag.get() > 0) {
                    headerCell.add(new Element("stats", "stats", "takesLag", getDurationBreakdown(totals.takesLag.get()), null));
                }
                if (totals.takeApplies.get() > 0) {
                    headerCell.add(new Element("stats", "stats", "takeApplies", numberFormat.format(totals.takeApplies.get()), null));
                }
                if (totals.takeAppliesLag.get() > 0) {
                    headerCell.add(new Element("stats", "stats", "takeAppliesLag", getDurationBreakdown(totals.takeAppliesLag.get()), null));
                }

            }

            for (String state : stateCounts.elementSet()) {
                int count = stateCounts.count(state);
                try {
                    State s = State.valueOf(state);
                    ((List) header.get(electionIndex)).add(new Element("election", "election", state, String.valueOf(count),
                        (s == State.leader || s == State.follower)
                            ? null : ((s == State.demoted || s == State.nominated || s == State.inactive) ? "warning" : "danger")
                    ));
                } catch (IllegalArgumentException x) {
                    ((List) header.get(electionIndex)).add(new Element("election", "election", state, String.valueOf(count), "danger"));
                }
            }

            data.put("unhealthy", input.unhealthy);
            data.put("unhealthyCount", unhealthyCount.toString());
            data.put("unhealthyRows", unhealthyRows);
            data.put("active", input.active);
            data.put("activeCount", activeCount.toString());
            data.put("activeRows", activeRows);
            data.put("system", input.system);
            data.put("systemCount", systemCount.toString());
            data.put("systemRows", systemRows);

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
