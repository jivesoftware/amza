package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.PartitionIsDisposedException;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.service.take.HighwaterStorage;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion.AmzaPartitionsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.getDurationBreakdown;

// soy.page.amzaPartitionsPluginRegion
public class AmzaPartitionsPluginRegion implements PageRegion<AmzaPartitionsPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingReader ringReader;
    private final AmzaService amzaService;

    public AmzaPartitionsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingReader ringReader,
        AmzaService amzaService) {
        this.template = template;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.amzaService = amzaService;
    }

    public static class AmzaPartitionsPluginRegionInput {

        final String action;
        final String ringName;
        final String partitionName;
        final Durability durability;
        final long tombstoneTimestampAgeInMillis;
        final long tombstoneTimestampIntervalMillis;
        final long tombstoneVersionAgeInMillis;
        final long tombstoneVersionIntervalMillis;
        final long ttlTimestampAgeInMillis;
        final long ttlTimestampIntervalMillis;
        final long ttlVersionAgeInMillis;
        final long ttlVersionIntervalMillis;
        final boolean forceCompactionOnStartup;
        final Consistency consistency;
        final boolean requireConsistency;
        final boolean replicated;
        final boolean disabled;
        final RowType rowType;
        final String indexClassName;
        final int maxValueSizeInIndex;
        final Map<String, String> indexProperties;
        final int updatesBetweenLeaps;
        final int maxLeaps;

        public AmzaPartitionsPluginRegionInput() {
            this.action = "";
            this.ringName = "";
            this.partitionName = "";
            durability = Durability.fsync_async;
            tombstoneTimestampAgeInMillis = 0;
            tombstoneTimestampIntervalMillis = 0;
            tombstoneVersionAgeInMillis = 0;
            tombstoneVersionIntervalMillis = 0;
            ttlTimestampAgeInMillis = 0;
            ttlTimestampIntervalMillis = 0;
            ttlVersionAgeInMillis = 0;
            ttlVersionIntervalMillis = 0;
            forceCompactionOnStartup = false;
            consistency = Consistency.leader_quorum;
            requireConsistency = true;
            replicated = true;
            disabled = false;
            rowType = RowType.snappy_primary;
            indexClassName = "lab";
            maxValueSizeInIndex = -1;
            indexProperties = null;
            updatesBetweenLeaps = -1;
            maxLeaps = -1;
        }

        public AmzaPartitionsPluginRegionInput(
            String action,
            String ringName,
            String partitionName,
            String durability,
            long tombstoneTimestampAgeInMillis,
            long tombstoneTimestampIntervalMillis,
            long tombstoneVersionAgeInMillis,
            long tombstoneVersionIntervalMillis,
            long ttlTimestampAgeInMillis,
            long ttlTimestampIntervalMillis,
            long ttlVersionAgeInMillis,
            long ttlVersionIntervalMillis,
            boolean forceCompactionOnStartup,
            String consistency,
            boolean requireConsistency,
            boolean replicated,
            boolean disabled,
            String rowType,
            String indexClassName,
            int maxValueSizeInIndex,
            Map<String, String> indexProperties,
            int updatesBetweenLeaps,
            int maxLeaps) {
            this.action = action;
            this.ringName = ringName;
            this.partitionName = partitionName;
            this.durability = Durability.valueOf(durability);
            this.tombstoneTimestampAgeInMillis = tombstoneTimestampAgeInMillis;
            this.tombstoneTimestampIntervalMillis = tombstoneTimestampIntervalMillis;
            this.tombstoneVersionAgeInMillis = tombstoneVersionAgeInMillis;
            this.tombstoneVersionIntervalMillis = tombstoneVersionIntervalMillis;
            this.ttlTimestampAgeInMillis = ttlTimestampAgeInMillis;
            this.ttlTimestampIntervalMillis = ttlTimestampIntervalMillis;
            this.ttlVersionAgeInMillis = ttlVersionAgeInMillis;
            this.ttlVersionIntervalMillis = ttlVersionIntervalMillis;
            this.forceCompactionOnStartup = forceCompactionOnStartup;
            this.consistency = Consistency.valueOf(consistency);
            this.requireConsistency = requireConsistency;
            this.replicated = replicated;
            this.disabled = disabled;
            this.rowType = RowType.valueOf(rowType);
            this.indexClassName = indexClassName;
            this.maxValueSizeInIndex = maxValueSizeInIndex;
            this.indexProperties = indexProperties;
            this.updatesBetweenLeaps = updatesBetweenLeaps;
            this.maxLeaps = maxLeaps;
        }

    }

    @Override
    public String render(AmzaPartitionsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            byte[] ringNameBytes = input.ringName.getBytes();
            byte[] partitionNameBytes = input.partitionName.getBytes();

            if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);

                switch (input.action) {
                    case "add":
                        LOG.info("Adding {}", partitionName);
                        amzaService.getRingWriter().ensureMaximalRing(ringNameBytes, 0);
                        amzaService.createPartitionIfAbsent(
                            partitionName,
                            new PartitionProperties(
                                input.durability,
                                input.tombstoneTimestampAgeInMillis,
                                input.tombstoneTimestampIntervalMillis,
                                input.tombstoneVersionAgeInMillis,
                                input.tombstoneVersionIntervalMillis,
                                input.ttlTimestampAgeInMillis,
                                input.ttlTimestampIntervalMillis,
                                input.ttlVersionAgeInMillis,
                                input.ttlVersionIntervalMillis,
                                input.forceCompactionOnStartup,
                                input.consistency,
                                input.requireConsistency,
                                input.replicated,
                                input.disabled,
                                input.rowType,
                                input.indexClassName,
                                input.maxValueSizeInIndex,
                                input.indexProperties,
                                input.updatesBetweenLeaps,
                                input.maxLeaps));
                        amzaService.awaitOnline(partitionName, TimeUnit.SECONDS.toMillis(30));
                        break;

                    case "promote":
                        LOG.info("Promoting {}", partitionName);
                        amzaService.promotePartition(partitionName);
                        break;

                    case "remove":
                        LOG.info("Removing {}", partitionName);
                        amzaService.destroyPartition(partitionName);
                        break;

                    case "update":
                        amzaService.updateProperties(
                            partitionName,
                            new PartitionProperties(
                                input.durability,
                                input.tombstoneTimestampAgeInMillis,
                                input.tombstoneTimestampIntervalMillis,
                                input.tombstoneVersionAgeInMillis,
                                input.tombstoneVersionIntervalMillis,
                                input.ttlTimestampAgeInMillis,
                                input.ttlTimestampIntervalMillis,
                                input.ttlVersionAgeInMillis,
                                input.ttlVersionIntervalMillis,
                                input.forceCompactionOnStartup,
                                input.consistency,
                                input.requireConsistency,
                                input.replicated,
                                input.disabled,
                                input.rowType,
                                input.indexClassName,
                                input.maxValueSizeInIndex,
                                input.indexProperties,
                                input.updatesBetweenLeaps,
                                input.maxLeaps));
                        break;
                }
            }

            List<PartitionName> partitionNames = Lists.newArrayList();
            Iterables.addAll(partitionNames, amzaService.getSystemPartitionNames());
            Iterables.addAll(partitionNames, amzaService.getMemberPartitionNames());
            partitionNames.sort((o1, o2) -> {
                int i = -Boolean.compare(o1.isSystemPartition(), o2.isSystemPartition());
                if (i != 0) {
                    return i;
                }
                i = UnsignedBytes.lexicographicalComparator().compare(o1.getRingName(), o2.getRingName());
                if (i != 0) {
                    return i;
                }
                i = UnsignedBytes.lexicographicalComparator().compare(o1.getName(), o2.getName());
                if (i != 0) {
                    return i;
                }
                return i;
            });

            List<Map<String, Object>> rows = new ArrayList<>();

            if (!input.ringName.isEmpty() || !input.partitionName.isEmpty()) {
                long start = System.currentTimeMillis();
                AtomicLong hits = new AtomicLong();
                AtomicLong missed = new AtomicLong();
                PartitionStripeProvider partitionStripeProvider = amzaService.getPartitionStripeProvider();

                for (PartitionName partitionName : partitionNames) {
                    String pn = new String(partitionName.getName());
                    String prn = new String(partitionName.getRingName());

                    if ((input.partitionName.isEmpty() || pn.contains(input.partitionName) || input.partitionName.equals("*"))
                        && (input.ringName.isEmpty() || prn.contains(input.ringName) || input.ringName.equals("*"))) {

                        hits.incrementAndGet();

                        Map<String, Object> row = new HashMap<>();
                        row.put("type", partitionName.isSystemPartition() ? "SYSTEM" : "USER");
                        row.put("name", pn);
                        row.put("ringName", prn);

                        RingTopology ring = ringReader.getRing(partitionName.getRingName(), -1);
                        List<Map<String, Object>> ringHosts = new ArrayList<>();
                        for (RingMemberAndHost entry : ring.entries) {
                            Map<String, Object> ringHost = new HashMap<>();
                            ringHost.put("member", entry.ringMember.getMember());
                            ringHost.put("host", entry.ringHost.getHost());
                            ringHost.put("port", String.valueOf(entry.ringHost.getPort()));
                            ringHosts.add(ringHost);
                        }
                        row.put("ring", ringHosts);

                        PartitionProperties partitionProperties = amzaService.getPartitionProperties(partitionName);
                        if (partitionProperties == null) {
                            row.put("durability", Durability.fsync_async);
                            row.put("tombstoneTimestampAgeInMillis", 0);
                            row.put("tombstoneTimestampIntervalMillis", 0);
                            row.put("tombstoneVersionAgeInMillis", 0);
                            row.put("tombstoneVersionIntervalMillis", 0);
                            row.put("ttlTimestampAgeInMillis", 0);
                            row.put("ttlTimestampIntervalMillis", 0);
                            row.put("ttlVersionAgeInMillis", 0);
                            row.put("ttlVersionIntervalMillis", 0);
                            row.put("forceCompactionOnStartup", false);
                            row.put("consistency", Consistency.leader_quorum);
                            row.put("requireConsistency", true);
                            row.put("replicated", true);
                            row.put("disabled", false);
                            row.put("rowType", partitionProperties.rowType);
                            row.put("indexClassName", "lab");
                            row.put("maxValueSizeInIndex", -1);
                            row.put("updatesBetweenLeaps", -1);
                            row.put("maxLeaps", -1);
                        } else {
                            row.put("durability", partitionProperties.durability);
                            row.put("tombstoneTimestampAgeInMillis", String.valueOf(partitionProperties.tombstoneTimestampAgeInMillis));
                            row.put("tombstoneTimestampIntervalMillis", String.valueOf(partitionProperties.tombstoneTimestampIntervalMillis));
                            row.put("tombstoneVersionAgeInMillis", String.valueOf(partitionProperties.tombstoneVersionAgeInMillis));
                            row.put("tombstoneVersionIntervalMillis", String.valueOf(partitionProperties.tombstoneVersionIntervalMillis));
                            row.put("ttlTimestampAgeInMillis", String.valueOf(partitionProperties.ttlTimestampAgeInMillis));
                            row.put("ttlTimestampIntervalMillis", String.valueOf(partitionProperties.ttlTimestampIntervalMillis));
                            row.put("ttlVersionAgeInMillis", String.valueOf(partitionProperties.ttlVersionAgeInMillis));
                            row.put("ttlVersionIntervalMillis", String.valueOf(partitionProperties.ttlVersionIntervalMillis));
                            row.put("forceCompactionOnStartup", partitionProperties.forceCompactionOnStartup);
                            row.put("consistency", partitionProperties.consistency);
                            row.put("requireConsistency", partitionProperties.requireConsistency);
                            row.put("replicated", partitionProperties.replicated);
                            row.put("disabled", partitionProperties.disabled);
                            row.put("rowType", partitionProperties.rowType);
                            row.put("indexClassName", partitionProperties.indexClassName);
                            row.put("maxValueSizeInIndex", partitionProperties.maxValueSizeInIndex);
                            row.put("updatesBetweenLeaps", partitionProperties.updatesBetweenLeaps);
                            row.put("maxLeaps", partitionProperties.maxLeaps);
                        }

                        try {
                            partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                                VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                                if (partitionName.isSystemPartition()) {
                                    HighwaterStorage systemHighwaterStorage = amzaService.getSystemHighwaterStorage();
                                    WALHighwater partitionHighwater = systemHighwaterStorage.getPartitionHighwater(versionedPartitionName, true);
                                    row.put("highwaters", renderHighwaters(partitionHighwater));
                                } else {
                                    WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName, true);
                                    row.put("highwaters", renderHighwaters(partitionHighwater));
                                }
                                return null;
                            });
                        } catch (PartitionIsDisposedException | PropertiesNotPresentException e) {
                            row.put("highwaters", "partition-disposed");
                        }

                        rows.add(row);
                    } else {
                        missed.incrementAndGet();
                    }
                }

                data.put("message", "Found " + hits + "/" + missed + " in " + getDurationBreakdown(System.currentTimeMillis() - start));
                data.put("messageType", "info");
            } else {
                data.put("message", "Please input Name or Ring Name");
                data.put("messageType", "info");
            }

            data.put("partitions", rows);
        } catch (Exception e) {
            data.put("partitions", new ArrayList<>());
            data.put("message", e.getLocalizedMessage());
            data.put("messageType", "warning");
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    private String renderHighwaters(WALHighwater walHighwater) {
        StringBuilder sb = new StringBuilder();
        for (WALHighwater.RingMemberHighwater e : walHighwater.ringMemberHighwater) {
            sb.append("<p>");
            sb.append(e.ringMember.getMember()).append("=").append(Long.toHexString(e.transactionId)).append("\n");
            sb.append("</p>");
        }
        return sb.toString();
    }

    @Override
    public String getTitle() {
        return "Amza Partitions";
    }

}
