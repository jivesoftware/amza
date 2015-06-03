package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.shared.ring.AmzaRing;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
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
        final String member;
        final String action;

        public AmzaRingsPluginRegionInput(String ringName, String status, String member, String action) {
            this.ringName = ringName;
            this.status = status;
            this.member = member;
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
                    amzaRing.addRingMember(input.ringName, new RingMember(input.member));
                } else if (input.action.equals("remove")) {
                    amzaRing.removeRingMember(input.ringName, new RingMember(input.member));
                }

                final List<Map<String, String>> rows = new ArrayList<>();
                amzaRing.allRings((String ringName, RingMember ringMember, RingHost ringHost) -> {
                    if ((input.ringName.isEmpty() || ringName.contains(input.ringName))
                        && (input.member.isEmpty() || "".contains(input.member))
                        && (input.status.isEmpty() || "".contains(input.status))) {

                        Map<String, String> row = new HashMap<>();
                        row.put("ringName", ringName);
                        row.put("status", "online");
                        row.put("member", ringMember.getMember());
                        row.put("host", ringHost.getHost());
                        row.put("port", String.valueOf(ringHost.getPort()));
                        rows.add(row);
                    }
                    return true;
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
        return "Amza Rings";
    }

}
