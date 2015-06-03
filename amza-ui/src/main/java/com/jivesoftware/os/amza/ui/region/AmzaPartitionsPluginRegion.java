package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.partition.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.AmzaRing;
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
// soy.page.amzaRingPluginRegion
public class AmzaPartitionsPluginRegion implements PageRegion<Optional<AmzaPartitionsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRing amzaRing;
    private final AmzaService amzaService;

    public AmzaPartitionsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRing amzaRing,
        AmzaService amzaService) {
        this.template = template;
        this.renderer = renderer;
        this.amzaRing = amzaRing;
        this.amzaService = amzaService;
    }

    public static class AmzaPartitionsPluginRegionInput {

        final String host;
        final String port;
        final String action;

        public AmzaPartitionsPluginRegionInput(String host, String port, String action) {
            this.host = host;
            this.port = port;
            this.action = action;
        }

    }

    @Override
    public String render(Optional<AmzaPartitionsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (optionalInput.isPresent()) {
                AmzaPartitionsPluginRegionInput input = optionalInput.get();

                if (input.action.equals("add")) {

                } else if (input.action.equals("remove")) {

                } else if (input.action.equals("update")) {

                }

                List<Map<String, Object>> rows = new ArrayList<>();
                Set<PartitionName> partitionNames = amzaService.getPartitionNames();
                for (PartitionName partitionName : partitionNames) {

                    Map<String, Object> row = new HashMap<>();
                    row.put("type", partitionName.isSystemPartition()? "SYSTEM" : "USER");
                    row.put("name", partitionName.getPartitionName());
                    row.put("ringName", partitionName.getRingName());

                    NavigableMap<RingMember, RingHost> ring = amzaRing.getRing(partitionName.getRingName());
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
                        row.put("replicationFactor", "?");
                        row.put("takeFromFactor", "?");

                        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                            new PrimaryIndexDescriptor("memory", 0, false, null), null, 1000, 1000);
                        row.put("walStorageDescriptor", walStorageDescriptor(storageDescriptor));

                    } else {
                        row.put("disabled", partitionProperties.disabled);
                        row.put("replicationFactor", partitionProperties.replicationFactor);
                        row.put("takeFromFactor", partitionProperties.takeFromFactor);
                        row.put("walStorageDescriptor", walStorageDescriptor(partitionProperties.walStorageDescriptor));
                    }

                    rows.add(row);
                }

                data.put("regions", rows);

            }
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
        return "Amza Regions";
    }

}
