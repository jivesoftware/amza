package com.jivesoftware.os.amza.ui.region;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.getDurationBreakdown;

/**
 *
 */
// soy.page.amzaRingPluginRegion
public class AmzaRingsPluginRegion implements PageRegion<AmzaRingsPluginRegion.AmzaRingsPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingWriter ringWriter;
    private final AmzaRingReader ringReader;

    public AmzaRingsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingWriter ringWriter,
        AmzaRingReader ringReader) {
        this.template = template;
        this.renderer = renderer;
        this.ringWriter = ringWriter;
        this.ringReader = ringReader;
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
    public String render(AmzaRingsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {

            if (input.action.equals("add")) {
                ringWriter.addRingMember(input.ringName.getBytes(), new RingMember(input.member));
            } else if (input.action.equals("remove")) {
                ringWriter.removeRingMember(input.ringName.getBytes(), new RingMember(input.member));
            }

            List<Map<String, String>> rows = new ArrayList<>();
            if (!input.ringName.isEmpty() || !input.member.isEmpty()) {
                long start = System.currentTimeMillis();
                AtomicLong hits = new AtomicLong();
                AtomicLong missed = new AtomicLong();
                ringReader.allRings((byte[] ringName, RingMember ringMember, RingHost ringHost) -> {
                    if ((input.ringName.isEmpty() || new String(ringName).contains(input.ringName))
                        && (input.member.isEmpty() || ringMember.getMember().contains(input.member))) {

                        Map<String, String> row = new HashMap<>();
                        row.put("ringName", new String(ringName));
                        row.put("member", ringMember.getMember());
                        row.put("host", ringHost.getHost());
                        row.put("port", String.valueOf(ringHost.getPort()));
                        rows.add(row);
                        hits.incrementAndGet();
                    } else {
                        missed.incrementAndGet();
                    }
                    return true;
                });
                data.put("message", "Found " + hits + "/" + missed + " in " + getDurationBreakdown(System.currentTimeMillis() - start));
                data.put("messageType", "info");
            } else {
                data.put("message", "Please input Ring or Logical Name");
                data.put("messageType", "info");
            }
            data.put("rings", rows);
        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Amza Rings";
    }

}
