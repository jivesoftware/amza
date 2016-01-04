package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
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
    private final AmzaRingWriter ringWriter;
    private final AmzaRingReader ringReader;

    public AmzaClusterPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingWriter ringWriter,
        AmzaRingReader ringReader) {
        this.template = template;
        this.renderer = renderer;
        this.ringWriter = ringWriter;
        this.ringReader = ringReader;
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
                    ringWriter.register(new RingMember(input.member), new RingHost(input.host, input.port), -1);
                } else if (input.action.equals("remove")) {
                    ringWriter.deregister(new RingMember(input.member));
                }

                List<Map<String, String>> rows = new ArrayList<>();
                RingTopology ring = ringReader.getRing(AmzaRingReader.SYSTEM_RING);
                for (RingMemberAndHost entry : ring.entries) {
                    Map<String, String> row = new HashMap<>();
                    row.put("member", entry.ringMember.getMember());
                    row.put("host", entry.ringHost.getHost());
                    row.put("port", String.valueOf(entry.ringHost.getPort()));
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
