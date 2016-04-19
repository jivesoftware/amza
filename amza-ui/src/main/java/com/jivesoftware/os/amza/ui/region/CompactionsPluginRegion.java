package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 *
 */
// soy.page.compactionsPluginRegion
public class CompactionsPluginRegion implements PageRegion<CompactionsPluginRegion.CompactionsPluginRegionInput> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaRingReader ringReader;
    private final AmzaService amzaService;
    private final AmzaStats amzaStats;

    public CompactionsPluginRegion(String template,
        SoyRenderer renderer,
        AmzaRingReader ringReader,
        AmzaService amzaService,
        AmzaStats amzaStats) {
        this.template = template;
        this.renderer = renderer;
        this.ringReader = ringReader;
        this.amzaService = amzaService;
        this.amzaStats = amzaStats;

    }

    public static class CompactionsPluginRegionInput {

        final String action;

        public CompactionsPluginRegionInput(String action) {
            this.action = action;
        }
    }

    @Override
    public String render(CompactionsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (input.action.equals("rebalanceStripes")) {
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("rebalanceStripes-%d").build()).submit(() -> {
                    amzaService.rebalanceStripes();
                });
            }

            if (input.action.equals("forceCompactionDeltas")) {
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("forceCompactionDeltas-%d").build()).submit(() -> {
                    amzaService.mergeAllDeltas(true);
                });
            }
            if (input.action.equals("forceCompactionTombstones")) {
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("forceCompactionTombstones-%d").build()).submit(() -> {
                    amzaService.compactAllTombstones();
                    return null;
                });
            }
            if (input.action.equals("forceExpunge")) {
                Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("forceExpunge-%d").build()).submit(() -> {
                    amzaService.expunge();
                    return null;
                });
            }

            List<Map.Entry<String, Long>> ongoingCompactions = amzaStats.ongoingCompactions(AmzaStats.CompactionFamily.values());
            data.put("ongoingCompactions", (Object) Iterables.transform(Iterables.filter(ongoingCompactions, Predicates.notNull()),
                (Map.Entry<String, Long> input1) -> ImmutableMap.of("name",
                    input1.getKey(), "elapse", numberFormat.format(input1.getValue()))));

            List<Map.Entry<String, Long>> recentCompaction = amzaStats.recentCompaction();
            data.put("recentCompactions", (Object) Iterables.transform(Iterables.filter(recentCompaction, Predicates.notNull()),
                (Map.Entry<String, Long> input1) -> ImmutableMap.of("name",
                    input1.getKey(), "elapse", numberFormat.format(input1.getValue()))));
            long compactionTotal = 0;
            for (AmzaStats.CompactionFamily compactionFamily : AmzaStats.CompactionFamily.values()) {
                compactionTotal += amzaStats.getTotalCompactions(compactionFamily);
            }
            data.put("totalCompactions", numberFormat.format(compactionTotal));

        } catch (Exception e) {
            log.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Compactions";
    }

}
