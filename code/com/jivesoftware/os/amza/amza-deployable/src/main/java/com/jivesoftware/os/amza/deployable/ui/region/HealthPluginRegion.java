package com.jivesoftware.os.amza.deployable.ui.region;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.deployable.ui.soy.SoyRenderer;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.Totals;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
// soy.page.healthPluginRegion
public class HealthPluginRegion implements PageRegion<Optional<HealthPluginRegion.HealthPluginRegionInput>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRing amzaRing;
    private final AmzaInstance amzaInstance;
    private final AmzaStats amzaStats;

    public HealthPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRing amzaRing,
        AmzaInstance amzaInstance,
        AmzaStats amzaStats
    ) {
        this.template = template;
        this.renderer = renderer;
        this.amzaRing = amzaRing;
        this.amzaInstance = amzaInstance;
        this.amzaStats = amzaStats;
    }

    public static class HealthPluginRegionInput {

        final String cluster;
        final String host;
        final String service;

        public HealthPluginRegionInput(String cluster, String host, String service) {
            this.cluster = cluster;
            this.host = host;
            this.service = service;
        }
    }

    @Override
    public String render(Optional<HealthPluginRegionInput> optionalInput) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            HealthPluginRegionInput input = optionalInput.get();

            List<Map.Entry<String, Long>> ongoingCompactions = amzaStats.ongoingCompactions();
            data.put("ongoingCompactions", (Object) Lists.transform(ongoingCompactions, new Function<Map.Entry<String, Long>, Map<String, String>>() {

                @Override
                public Map<String, String> apply(Map.Entry<String, Long> input) {
                    return ImmutableMap.of("name", input.getKey(), "elapse", String.valueOf(input.getValue()));
                }
            }));

            List<Map.Entry<String, Long>> recentCompaction = amzaStats.recentCompaction();
            data.put("recentCompactions", (Object) Lists.transform(recentCompaction, new Function<Map.Entry<String, Long>, Map<String, String>>() {

                @Override
                public Map<String, String> apply(Map.Entry<String, Long> input) {
                    return ImmutableMap.of("name", input.getKey(), "elapse", String.valueOf(input.getValue()));
                }
            }));
            data.put("totalCompactions", String.valueOf(amzaStats.getTotalCompactions()));

            data.put("grandTotals", regionTotals(null, amzaStats.getGrandTotal()));
            List<Map<String, Object>> regionTotals = new ArrayList<>();
            for (Map.Entry<RegionName, Totals> totals : amzaStats.getRegionTotals().entrySet()) {
                regionTotals.add(regionTotals(totals.getKey(), totals.getValue()));
            }
            data.put("regionTotals", regionTotals);

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    public Map<String, Object> regionTotals(RegionName name, AmzaStats.Totals totals) throws Exception {
        Map<String, Object> map = new HashMap<>();
        if (name != null) {
            map.put("name", name.getRegionName());
            map.put("ringName", name.getRingName());
            List<RingHost> ring = amzaRing.getRing(name.getRingName());
            List<Map<String, String>> ringMaps = new ArrayList<>();
            for (RingHost r : ring) {
                ringMaps.add(ImmutableMap.of("host", r.getHost(), "port", String.valueOf(r.getPort())));
            }
            map.put("ring", ringMaps);

            map.put("minKey", name.getMinKeyInclusive() == null ? "" : Arrays.toString(name.getMinKeyInclusive().getKey()));
            map.put("maxKey", name.getMaxKeyExclusive() == null ? "" : Arrays.toString(name.getMaxKeyExclusive().getKey()));
        }
        map.put("applies", String.valueOf(totals.applies.get()));
        map.put("appliesLag", String.valueOf(totals.appliesLag.get()));
        map.put("gets", String.valueOf(totals.gets.get()));
        map.put("getsLag", String.valueOf(totals.getsLag.get()));
        map.put("scans", String.valueOf(totals.scans.get()));
        map.put("scansLag", String.valueOf(totals.scansLag.get()));
        map.put("received", String.valueOf(totals.received.get()));
        map.put("receivedLag", String.valueOf(totals.receivedLag.get()));
        map.put("replicates", String.valueOf(totals.replicates.get()));
        map.put("replicatesLag", String.valueOf(totals.replicatesLag.get()));
        map.put("takeUpdates", String.valueOf(totals.takeUpdates.get()));
        map.put("takeUpdatesLag", String.valueOf(totals.takeUpdatesLag.get()));

        return map;
    }

    @Override
    public String getTitle() {
        return "Health";
    }

}
