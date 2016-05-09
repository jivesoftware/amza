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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
// soy.page.amzaPartitionsPluginRegion
public class AmzaPartitionsPluginRegion implements PageRegion<AmzaPartitionsPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

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
        final String indexClassName;
        final String partitionName;
        final String consistency;
        final boolean requireConsistency;
        final boolean replicated;
        final RowType rowType;

        public AmzaPartitionsPluginRegionInput(String action,
            String ringName,
            String indexClassName,
            String partitionName,
            String consistency,
            boolean requireConsistency,
            boolean replicated,
            RowType rowType) {
            this.action = action;
            this.ringName = ringName;
            this.indexClassName = indexClassName;
            this.partitionName = partitionName;
            this.consistency = consistency;
            this.requireConsistency = requireConsistency;
            this.replicated = replicated;
            this.rowType = rowType;
        }

    }

    @Override
    public String render(AmzaPartitionsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            byte[] ringNameBytes = input.ringName.getBytes();
            byte[] partitionNameBytes = input.partitionName.getBytes();
            if (input.action.equals("add")) {
                if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                    amzaService.getRingWriter().ensureMaximalRing(ringNameBytes);

                    PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);
                    amzaService.createPartitionIfAbsent(partitionName,
                        new PartitionProperties(Durability.fsync_never,
                            0, 0, 0, 0, 0, 0, 0, 0,
                            false,
                            Consistency.valueOf(input.consistency),
                            input.requireConsistency,
                            input.replicated,
                            false,
                            input.rowType,
                            input.indexClassName,
                            null,
                            -1,
                            -1));
                    amzaService.awaitOnline(partitionName, TimeUnit.SECONDS.toMillis(30));
                }
            } else if (input.action.equals("promote")) {
                if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                    PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);
                    log.info("Promoting {}", partitionName);
                    amzaService.promotePartition(partitionName);
                }
            } else if (input.action.equals("remove")) {
                if (ringNameBytes.length > 0 && partitionNameBytes.length > 0) {
                    PartitionName partitionName = new PartitionName(false, ringNameBytes, partitionNameBytes);
                    log.info("Removing {}", partitionName);
                    amzaService.destroyPartition(partitionName);
                }
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            List<PartitionName> partitionNames = Lists.newArrayList();
            Iterables.addAll(partitionNames, amzaService.getSystemPartitionNames());
            Iterables.addAll(partitionNames, amzaService.getMemberPartitionNames());
            Collections.sort(partitionNames, (o1, o2) -> {
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
            PartitionStripeProvider partitionStripeProvider = amzaService.getPartitionStripeProvider();
            for (PartitionName partitionName : partitionNames) {

                Map<String, Object> row = new HashMap<>();
                row.put("type", partitionName.isSystemPartition() ? "SYSTEM" : "USER");
                row.put("name", new String(partitionName.getName()));
                row.put("ringName", new String(partitionName.getRingName()));

                RingTopology ring = ringReader.getRing(partitionName.getRingName());
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
                    row.put("disabled", "?");
                    row.put("consistency", "none");
                    row.put("requireConsistency", "true");
                    row.put("takeFromFactor", "?");

                    row.put("partitionProperties", "none");

                } else {
                    row.put("disabled", partitionProperties.disabled);
                    row.put("replicated", partitionProperties.replicated);
                    row.put("consistency", partitionProperties.consistency.name());
                    row.put("requireConsistency", partitionProperties.requireConsistency);
                    row.put("partitionProperties", partitionProperties.toString());
                }

                try {
                    partitionStripeProvider.txPartition(partitionName, (txPartitionStripe, highwaterStorage, versionedAquarium) -> {
                        VersionedPartitionName versionedPartitionName = versionedAquarium.getVersionedPartitionName();
                        if (partitionName.isSystemPartition()) {
                            HighwaterStorage systemHighwaterStorage = amzaService.getSystemHighwaterStorage();
                            WALHighwater partitionHighwater = systemHighwaterStorage.getPartitionHighwater(versionedPartitionName);
                            row.put("highwaters", renderHighwaters(partitionHighwater));
                        } else {
                            WALHighwater partitionHighwater = highwaterStorage.getPartitionHighwater(versionedPartitionName);
                            row.put("highwaters", renderHighwaters(partitionHighwater));
                        }
                        return null;
                    });
                } catch (PartitionIsDisposedException e) {
                    row.put("highwaters", "partition-disposed");
                } catch (PropertiesNotPresentException e) {
                    row.put("highwaters", "properties-disposed");
                }

                rows.add(row);
            }

            data.put("partitions", rows);

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
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

    @Override
    public String getTitle() {
        return "Amza Partitions";
    }

}
