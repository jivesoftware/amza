package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.replication.PartitionStatusStorage;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.ui.region.AmzaPartitionsPluginRegion.AmzaPartitionsPluginRegionInput;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 *
 */
// soy.page.amzaPartitionsPluginRegion
public class AmzaPartitionsPluginRegion implements PageRegion<AmzaPartitionsPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

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

        public AmzaPartitionsPluginRegionInput(String action, String ringName, String partitionName) {
            this.action = action;
            this.ringName = ringName;
            this.partitionName = partitionName;
        }

    }

    @Override
    public String render(AmzaPartitionsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            if (input.action.equals("add")) {

            } else if (input.action.equals("remove")) {
                PartitionName partitionName = new PartitionName(false, input.ringName, input.partitionName);
                log.info("Removing {}", partitionName);
                amzaService.destroyPartition(partitionName);
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            Set<PartitionName> partitionNames = amzaService.getPartitionNames();
            for (PartitionName partitionName : partitionNames) {

                Map<String, Object> row = new HashMap<>();
                PartitionStatusStorage.VersionedStatus status = amzaService.getPartitionStatusStorage().getLocalStatus(partitionName);
                row.put("status", status == null ? "unknown" : status.status.toString());
                row.put("version", status == null ? "unknown" : String.valueOf(status.version));
                row.put("type", partitionName.isSystemPartition() ? "SYSTEM" : "USER");
                row.put("name", partitionName.getName());
                row.put("ringName", partitionName.getRingName());

                NavigableMap<RingMember, RingHost> ring = ringReader.getRing(partitionName.getRingName());
                List<Map<String, Object>> ringHosts = new ArrayList<>();
                for (Map.Entry<RingMember, RingHost> r : ring.entrySet()) {
                    Map<String, Object> ringHost = new HashMap<>();
                    ringHost.put("member", r.getKey().getMember());
                    ringHost.put("host", r.getValue().getHost());
                    ringHost.put("port", String.valueOf(r.getValue().getPort()));
                    ringHosts.add(ringHost);
                }
                row.put("ring", ringHosts);

                PartitionProperties partitionProperties = amzaService.getPartitionProperties(partitionName);
                if (partitionProperties == null) {
                    row.put("disabled", "?");
                    row.put("takeFromFactor", "?");

                    WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                        new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
                    row.put("walStorageDescriptor", walStorageDescriptor(storageDescriptor));

                } else {
                    row.put("disabled", partitionProperties.disabled);
                    row.put("takeFromFactor", partitionProperties.takeFromFactor);
                    row.put("walStorageDescriptor", walStorageDescriptor(partitionProperties.walStorageDescriptor));
                }

                rows.add(row);
            }

            data.put("partitions", rows);

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    Map<String, Object> walStorageDescriptor(WALStorageDescriptor storageDescriptor) {
        Map<String, Object> map = new HashMap<>();
        map.put("maxUpdatesBetweenCompactionHintMarker", String.valueOf(storageDescriptor.maxUpdatesBetweenCompactionHintMarker));
        map.put("maxUpdatesBetweenIndexCommitMarker", String.valueOf(storageDescriptor.maxUpdatesBetweenIndexCommitMarker));
        map.put("primaryIndexDescriptor", primaryIndexDescriptor(storageDescriptor.primaryIndexDescriptor));

        List<Map<String, Object>> secondary = new ArrayList<>();
        if (storageDescriptor.secondaryIndexDescriptors != null) {
            for (SecondaryIndexDescriptor secondaryIndexDescriptor : storageDescriptor.secondaryIndexDescriptors) {
                secondary.add(secondaryIndexDescriptor(secondaryIndexDescriptor));
            }
        }
        map.put("secondaryIndexDescriptor", secondary);
        return map;
    }

    Map<String, Object> primaryIndexDescriptor(PrimaryIndexDescriptor primaryIndexDescriptor) {

        Map<String, Object> map = new HashMap<>();
        map.put("className", primaryIndexDescriptor.className);
        map.put("ttlInMillis", String.valueOf(primaryIndexDescriptor.ttlInMillis));
        map.put("forceCompactionOnStartup", primaryIndexDescriptor.forceCompactionOnStartup);
        //public Map<String, String> properties; //TODO
        return map;
    }

    Map<String, Object> secondaryIndexDescriptor(SecondaryIndexDescriptor secondaryIndexDescriptor) {
        Map<String, Object> map = new HashMap<>();
        map.put("className", secondaryIndexDescriptor.className);
        //public Map<String, String> properties; //TODO
        return map;
    }

    @Override
    public String getTitle() {
        return "Amza Partitions";
    }

}
