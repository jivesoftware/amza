package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.stats.AmzaStats.CompactionStats;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.jivesoftware.os.amza.ui.region.MetricsPluginRegion.getDurationBreakdown;

/**
 *
 */
// soy.page.compactionsPluginRegion
public class CompactionsPluginRegion implements PageRegion<CompactionsPluginRegion.CompactionsPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
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
        final String fromIndexClass;
        final String toIndexClass;

        public CompactionsPluginRegionInput(String action, String fromIndexClass, String toIndexClass) {
            this.action = action;
            this.fromIndexClass = fromIndexClass;
            this.toIndexClass = toIndexClass;
        }
    }

    @Override
    public String render(CompactionsPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (input.action.equals("rebalanceStripes")) {
                new Thread(
                    () -> {
                        amzaService.rebalanceStripes();
                    }, "rebalanceStripes"
                ).start();
            }

            if (input.action.equals("forceCompactionDeltas")) {
                new Thread(
                    () -> {
                        amzaService.mergeAllDeltas(true);
                    }, "forceCompactionDeltas"
                ).start();
            }
            if (input.action.equals("forceCompactionTombstones")) {
                new Thread(
                    () -> {
                        try {
                            amzaService.compactAllTombstones();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, "forceCompactionTombstones"
                ).start();
            }
            if (input.action.equals("forceExpunge")) {
                new Thread(
                    () -> {
                        try {
                            amzaService.expunge();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, "forceExpunge"
                ).start();
            }

            if (input.action.equals("migrateIndexClass")) {
                amzaService.migrate(input.fromIndexClass, input.toIndexClass);
            }

            List<Map.Entry<String, CompactionStats>> ongoingCompactions = amzaStats.ongoingCompactions(AmzaStats.CompactionFamily.values());
            data.put("ongoingCompactions", (Object) Iterables.transform(Iterables.filter(ongoingCompactions, Predicates.notNull()),
                (Map.Entry<String, CompactionStats> input1) -> {
                    return ImmutableMap.of("name", input1.getKey(),
                        "elapse", getDurationBreakdown(input1.getValue().elapse()),
                        "timers", Iterables.transform(Iterables.filter(input1.getValue().getTimings(), Predicates.notNull()),
                            (Map.Entry<String, Long> input2) -> {
                                return ImmutableMap.of("name", input2.getKey(), "value", getDurationBreakdown(input2.getValue()));
                            }),
                        "counters", Iterables.transform(Iterables.filter(input1.getValue().getCounts(), Predicates.notNull()),
                            (Map.Entry<String, Long> input2) -> {
                                return ImmutableMap.of("name", input2.getKey(), "value", numberFormat.format(input2.getValue()));
                            })
                    );
                }));

            List<Map.Entry<String, CompactionStats>> recentCompaction = amzaStats.recentCompaction();
            data.put("recentCompactions", (Object) Iterables.transform(Iterables.filter(recentCompaction, Predicates.notNull()),
                (Map.Entry<String, CompactionStats> input1) -> {
                    return ImmutableMap.of("name", input1.getKey(),
                        "age", getDurationBreakdown(System.currentTimeMillis() - input1.getValue().startTime()),
                        "duration", getDurationBreakdown(input1.getValue().duration()),
                        "timers", Iterables.transform(Iterables.filter(input1.getValue().getTimings(), Predicates.notNull()),
                            (Map.Entry<String, Long> input2) -> {
                                return ImmutableMap.of("name", input2.getKey(), "value", getDurationBreakdown(input2.getValue()));
                            }),
                        "counters", Iterables.transform(Iterables.filter(input1.getValue().getCounts(), Predicates.notNull()),
                            (Map.Entry<String, Long> input2) -> {
                                return ImmutableMap.of("name", input2.getKey(), "value", numberFormat.format(input2.getValue()));
                            })
                    );
                }));

            long compactionTotal = 0;
            for (AmzaStats.CompactionFamily compactionFamily : AmzaStats.CompactionFamily.values()) {
                compactionTotal += amzaStats.getTotalCompactions(compactionFamily);
            }
            data.put("totalCompactions", numberFormat.format(compactionTotal));

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    @Override
    public String getTitle() {
        return "Compactions";
    }

}
