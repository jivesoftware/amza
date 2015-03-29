package com.jivesoftware.os.amza.deployable.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.shared.AmzaRing;
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
// soy.page.amzaClusterPluginRegion
public class AmzaClusterPluginRegion implements PageRegion<Optional<AmzaClusterPluginRegion.AmzaClusterPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRing amzaRing;

    public AmzaClusterPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRing amzaRing) {
        this.template = template;
        this.renderer = renderer;
        this.amzaRing = amzaRing;
    }

    public static class AmzaClusterPluginRegionInput {

        final String host;
        final String port;
        final String action;

        public AmzaClusterPluginRegionInput(String host, String port, String action) {
            this.host = host;
            this.port = port;
            this.action = action;
        }

    }

    @Override
    public String render(Optional<AmzaClusterPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (optionalInput.isPresent()) {
                AmzaClusterPluginRegionInput input = optionalInput.get();

                if (input.action.equals("add")) {
                    amzaRing.addRingHost("system", new RingHost(input.host, Integer.parseInt(input.port)));
                } else if (input.action.equals("remove")) {
                    amzaRing.removeRingHost("system", new RingHost(input.host, Integer.parseInt(input.port)));
                }

                List<Map<String, String>> rows = new ArrayList<>();
                for (RingHost host : amzaRing.getRing("system")) {

                    Map<String, String> row = new HashMap<>();
                    row.put("host", host.getHost());
                    row.put("port", String.valueOf(host.getPort()));
                    rows.add(row);
                }

                data.put("ring", rows);

            }
        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Amza Cluster";
    }

}
