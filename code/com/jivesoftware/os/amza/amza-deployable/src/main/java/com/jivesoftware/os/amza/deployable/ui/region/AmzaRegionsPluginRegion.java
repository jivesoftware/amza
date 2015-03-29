package com.jivesoftware.os.amza.deployable.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.page.amzaRingPluginRegion
public class AmzaRegionsPluginRegion implements PageRegion<Optional<AmzaRegionsPluginRegion.AmzaRegionsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRing amzaRing;
    private final AmzaService amzaService;

    public AmzaRegionsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRing amzaRing,
        AmzaService amzaService) {
        this.template = template;
        this.renderer = renderer;
        this.amzaRing = amzaRing;
        this.amzaService = amzaService;
    }

    public static class AmzaRegionsPluginRegionInput {

        final String host;
        final String port;
        final String action;

        public AmzaRegionsPluginRegionInput(String host, String port, String action) {
            this.host = host;
            this.port = port;
            this.action = action;
        }

    }

    @Override
    public String render(Optional<AmzaRegionsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (optionalInput.isPresent()) {
                AmzaRegionsPluginRegionInput input = optionalInput.get();

                if (input.action.equals("add")) {

                } else if (input.action.equals("remove")) {

                } else if (input.action.equals("update")) {

                }

                List<Map<String, Object>> rows = new ArrayList<>();
                List<RegionName> regionNames = amzaService.getRegionNames();
                for (RegionName regionName : regionNames) {

                    Map<String, Object> row = new HashMap<>();
                    row.put("type", regionName.isSystemRegion() ? "SYSTEM" : "USER");
                    row.put("name", regionName.getRegionName());
                    row.put("ringName", regionName.getRingName());

                    List<RingHost> ring = amzaRing.getRing(regionName.getRingName());
                    List<Map<String, Object>> ringHosts = new ArrayList<>();
                    for (RingHost r : ring) {
                        Map<String, Object> ringHost = new HashMap<>();
                        ringHost.put("host", r.getHost());
                        ringHost.put("port", String.valueOf(r.getPort()));
                        ringHosts.add(ringHost);
                    }
                    row.put("ring", ringHosts);

                    RegionProperties regionProperties = amzaService.getRegionProperties(regionName);
                    if (regionProperties == null) {
                        row.put("disabled", "?");
                        row.put("replicationFactor", "?");
                        row.put("takeFromFactor", "?");

                        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(
                            new PrimaryIndexDescriptor("memory", Long.MAX_VALUE, false, null), null, 1000, 1000);
                        row.put("walStorageDescriptor", walStorageDescriptor(storageDescriptor));

                    } else {
                        row.put("disabled", regionProperties.disabled);
                        row.put("replicationFactor", regionProperties.replicationFactor);
                        row.put("takeFromFactor", regionProperties.takeFromFactor);
                        row.put("walStorageDescriptor", walStorageDescriptor(regionProperties.walStorageDescriptor));
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
