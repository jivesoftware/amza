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
// soy.page.amzaRingPluginRegion
public class AmzaRingPluginRegion implements PageRegion<Optional<AmzaRingPluginRegion.AmzaRingPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRing amzaRing;

    public AmzaRingPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRing amzaRing) {
        this.template = template;
        this.renderer = renderer;
        this.amzaRing = amzaRing;
    }

    public static class AmzaRingPluginRegionInput {

        final String host;
        final String port;
        final String action;

        public AmzaRingPluginRegionInput(String host, String port, String action) {
            this.host = host;
            this.port = port;
            this.action = action;
        }

    }

    @Override
    public String render(Optional<AmzaRingPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (optionalInput.isPresent()) {
                AmzaRingPluginRegionInput input = optionalInput.get();

                if (input.action.equals("add")) {
                    amzaRing.addRingHost("master", new RingHost(input.host, Integer.parseInt(input.port)));
                } else if (input.action.equals("remove")) {
                    amzaRing.removeRingHost("master", new RingHost(input.host, Integer.parseInt(input.port)));
                }

                List<Map<String, String>> rows = new ArrayList<>();
                for (RingHost host : amzaRing.getRing("master")) {

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
        return "Amza Ring";
    }

}
