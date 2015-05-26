package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

        final String member;
        final String host;
        final int port;
        final String action;

        public AmzaClusterPluginRegionInput(String member, String host, int port, String action) {
            this.member = member;
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
                    amzaRing.register(new RingMember(input.member), new RingHost(input.host, input.port));
                    amzaRing.addRingMember("system", new RingMember(input.member));
                } else if (input.action.equals("remove")) {
                    amzaRing.removeRingMember("system", new RingMember(input.member));
                    amzaRing.deregister(new RingMember(input.member));
                }

                List<Map<String, String>> rows = new ArrayList<>();
                for (Entry<RingMember, RingHost> node : amzaRing.getRing("system").entrySet()) {

                    Map<String, String> row = new HashMap<>();
                    row.put("member", node.getKey().getMember());
                    row.put("host", node.getValue().getHost());
                    row.put("port", String.valueOf(node.getValue().getPort()));
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
