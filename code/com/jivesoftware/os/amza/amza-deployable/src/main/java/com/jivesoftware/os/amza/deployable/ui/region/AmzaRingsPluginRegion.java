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
public class AmzaRingsPluginRegion implements PageRegion<Optional<AmzaRingsPluginRegion.AmzaRingsPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRing amzaRing;

    public AmzaRingsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRing amzaRing) {
        this.template = template;
        this.renderer = renderer;
        this.amzaRing = amzaRing;
    }

    public static class AmzaRingsPluginRegionInput {

        final String ringName;
        final String status;
        final String host;
        final String port;
        final String action;

        public AmzaRingsPluginRegionInput(String ringName, String status, String host, String port, String action) {
            this.ringName = ringName;
            this.status = status;
            this.host = host;
            this.port = port;
            this.action = action;
        }

    }

    @Override
    public String render(Optional<AmzaRingsPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (optionalInput.isPresent()) {
                final AmzaRingsPluginRegionInput input = optionalInput.get();

                if (input.action.equals("add")) {
                    amzaRing.addRingHost(input.ringName, new RingHost(input.host, Integer.parseInt(input.port)));
                } else if (input.action.equals("remove")) {
                    amzaRing.removeRingHost(input.ringName, new RingHost(input.host, Integer.parseInt(input.port)));
                }

                final List<Map<String, String>> rows = new ArrayList<>();
                amzaRing.allRings(new AmzaRing.RingStream() {

                    @Override
                    public boolean stream(String ringName, String status, RingHost ringHost) {
                        if ((input.ringName.isEmpty() || status.contains(input.ringName))
                            && (input.status.isEmpty() || status.contains(input.status))
                            && (input.host.isEmpty() || status.contains(input.host))
                            && (input.port.isEmpty() || status.contains(input.port))) {

                            Map<String, String> row = new HashMap<>();
                            row.put("ringName", ringName);
                            row.put("status", status);
                            row.put("host", ringHost.getHost());
                            row.put("port", String.valueOf(ringHost.getPort()));
                            rows.add(row);
                        }
                        return true;
                    }
                });
                data.put("rings", rows);
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
