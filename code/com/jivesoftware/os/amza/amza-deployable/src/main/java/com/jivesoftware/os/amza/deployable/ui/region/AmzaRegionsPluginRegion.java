package com.jivesoftware.os.amza.deployable.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
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

                    List<RingHost> ring = amzaRing.getRing(regionName.getRegionName());
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
                    } else {
                        row.put("disabled", regionProperties.disabled);
                        row.put("replicationFactor", regionProperties.replicationFactor);
                        row.put("takeFromFactor", regionProperties.takeFromFactor);

                        // TODO
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

    @Override
    public String getTitle() {
        return "Amza Regions";
    }

}
