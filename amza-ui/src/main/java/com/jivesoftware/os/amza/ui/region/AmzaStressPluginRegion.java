package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.storage.delta.DeltaOverCapacityException;
import com.jivesoftware.os.amza.shared.Partition;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
// soy.page.amzaStressPluginRegion
public class AmzaStressPluginRegion implements PageRegion<AmzaStressPluginRegion.AmzaStressPluginRegionInput> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final NumberFormat numberFormat = NumberFormat.getInstance();

    private static final ConcurrentMap<String, Stress> STRESS_MAP = Maps.newConcurrentMap();

    private final String template;
    private final SoyRenderer renderer;
    private final AmzaService amzaService;

    public AmzaStressPluginRegion(String template,
        SoyRenderer renderer,
        AmzaService amzaService) {
        this.template = template;
        this.renderer = renderer;
        this.amzaService = amzaService;
    }

    public static class AmzaStressPluginRegionInput {

        final String name;
        final String regionPrefix;
        final int numBatches;
        final int batchSize;
        final int numPartitions;
        final int numThreadsPerPartition;
        final int numKeyPrefixes;
        final int desiredQuorum;
        final boolean orderedInsertion;
        final String action;

        public AmzaStressPluginRegionInput(String name,
            String regionPrefix,
            int numBatches,
            int batchSize,
            int numPartitions,
            int numThreadsPerRegion,
            int numKeyPrefixes,
            int desiredQuorum,
            boolean orderedInsertion,
            String action) {
            this.name = name;
            this.regionPrefix = regionPrefix;
            this.numBatches = numBatches;
            this.batchSize = batchSize;
            this.numPartitions = numPartitions;
            this.numThreadsPerPartition = numThreadsPerRegion;
            this.numKeyPrefixes = numKeyPrefixes;
            this.desiredQuorum = desiredQuorum;
            this.orderedInsertion = orderedInsertion;
            this.action = action;
        }
    }

    @Override
    public String render(AmzaStressPluginRegionInput input) {
        Map<String, Object> data = Maps.newHashMap();

        try {
            if (input != null && !input.name.isEmpty() && !input.regionPrefix.isEmpty()) {

                if (input.action.equals("start") || input.action.equals("stop")) {
                    Stress stress = STRESS_MAP.remove(input.name);
                    LOG.info("Removed {}", input.name);
                    if (stress != null) {
                        boolean stopped = stress.stop(10, TimeUnit.SECONDS);
                        data.put("stopped", stopped);
                    }
                }
                if (input.action.equals("start")) {
                    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10);
                    Stress stress = new Stress(System.currentTimeMillis(), executor, new AtomicLong(0), input);
                    Stress existing = STRESS_MAP.putIfAbsent(input.name, stress);
                    LOG.info("Added {}", input.name);
                    if (existing == null) {
                        stress.start();
                    }
                }
            }

            List<Map<String, String>> rows = new ArrayList<>();
            for (Map.Entry<String, Stress> entry : STRESS_MAP.entrySet()) {
                Stress stress = entry.getValue();
                long added = stress.added.get();
                long endTimeMillis = stress.endTimeMillis.get();
                if (endTimeMillis < 0) {
                    endTimeMillis = System.currentTimeMillis();
                }
                long elapsed = endTimeMillis - stress.startTimeMillis;

                Map<String, String> row = new HashMap<>();
                row.put("name", entry.getKey());
                row.put("regionPrefix", stress.input.regionPrefix);
                row.put("numBatches", String.valueOf(stress.input.numBatches));
                row.put("batchSize", String.valueOf(stress.input.batchSize));
                row.put("numPartitions", String.valueOf(stress.input.numPartitions));
                row.put("numThreadsPerRegion", String.valueOf(stress.input.numThreadsPerPartition));
                row.put("numKeyPrefixes", String.valueOf(stress.input.numKeyPrefixes));
                row.put("desiredQuorum", String.valueOf(stress.input.desiredQuorum));
                row.put("orderedInsertion", String.valueOf(stress.input.orderedInsertion));

                row.put("elapsed", MetricsPluginRegion.getDurationBreakdown(elapsed));
                row.put("added", numberFormat.format(added));
                row.put("addedPerSecond", numberFormat.format((double) added * 1000 / (double) elapsed));
                rows.add(row);
            }

            data.put("stress", rows);

        } catch (Exception e) {
            LOG.error("Unable to retrieve data", e);
        }

        return renderer.render(template, data);
    }

    private class Stress {

        final long startTimeMillis;
        final ExecutorService executor;
        final AtomicLong added;
        final AmzaStressPluginRegionInput input;
        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicLong endTimeMillis = new AtomicLong(-1);
        final AtomicBoolean forcedStop = new AtomicBoolean(false);

        public Stress(long startTimeMillis, ExecutorService executor, AtomicLong added, AmzaStressPluginRegionInput input) {
            this.startTimeMillis = startTimeMillis;
            this.executor = executor;
            this.added = added;
            this.input = input;
        }

        private void start() throws Exception {

            for (int i = 0; i < input.numPartitions; i++) {
                String regionName = input.regionPrefix + i;
                int numThread = input.orderedInsertion ? 1 : input.numThreadsPerPartition;
                for (int j = 0; j < numThread; j++) {
                    executor.submit(new Feeder(regionName, j, input.orderedInsertion));
                }
            }
        }

        private class Feeder implements Runnable {

            AtomicInteger batch = new AtomicInteger();
            private final String regionName;
            private final int threadIndex;
            private final boolean orderedInsertion;

            public Feeder(String regionName, int threadIndex, boolean orderedInsertion) {
                this.regionName = regionName;
                this.threadIndex = threadIndex;
                this.orderedInsertion = orderedInsertion;
            }

            @Override
            public void run() {
                try {
                    int b = batch.incrementAndGet();
                    if (b <= input.numBatches && !forcedStop.get()) {
                        feed(regionName, b, threadIndex);
                        executor.submit(this);
                    } else {
                        completed();
                    }
                } catch (Exception x) {
                    LOG.error("Failed to feed for region {}", new Object[]{regionName}, x);
                }
            }
        }

        private void completed() {
            if (this.completed.incrementAndGet() == input.numPartitions * input.numThreadsPerPartition) {
                endTimeMillis.set(System.currentTimeMillis());
            }
        }

        private boolean stop(long timeout, TimeUnit unit) throws InterruptedException {
            forcedStop.set(true);
            executor.shutdownNow();
            return executor.awaitTermination(timeout, unit);
        }

        private void feed(String regionName, int batch, int threadIndex) throws Exception {
            Partition partition = createPartitionIfAbsent(regionName);

            while (true) {
                try {
                    byte[] prefix = input.numKeyPrefixes > 0 ? UIO.intBytes(batch % input.numKeyPrefixes) : null;
                    Consistency consistency = Consistency.leader;
                    if (input.desiredQuorum == 1) {
                        consistency = Consistency.leader_plus_one;
                    } else if (input.desiredQuorum > 1) {
                        consistency = Consistency.leader_quorum;
                    }
                    partition.commit(consistency, prefix, (Highwaters highwaters, UnprefixedTxKeyValueStream txKeyValueStream) -> {
                        if (input.orderedInsertion) {
                            String max = String.valueOf(input.numBatches * input.batchSize);
                            int bStart = batch * input.batchSize;
                            for (int b = bStart, c = 0; c < input.batchSize; b++, c++) {
                                String k = Strings.padEnd(String.valueOf(b), max.length(), '0');
                                txKeyValueStream.row(-1, k.getBytes(), ("v" + batch).getBytes(), -1, false, -1);
                            }
                        } else {
                            int bStart = threadIndex * input.batchSize;
                            int bEnd = bStart + input.batchSize;
                            for (int b = bStart; b < bEnd; b++) {
                                String k = String.valueOf(batch);
                                txKeyValueStream.row(-1, (b + "k" + k).getBytes(), (b + "v" + batch).getBytes(), -1, false, -1);
                            }
                        }
                        return true;
                    }, 30_000);
                    break;

                } catch (DeltaOverCapacityException de) {
                    Thread.sleep(100);
                    LOG.warn("Slowing stress for region:{} batch:{} thread:{}", new Object[]{regionName, batch, threadIndex});
                } catch (Exception x) {
                    LOG.warn("Failed to set region:{} batch:{} thread:{}", new Object[]{regionName, batch, threadIndex}, x);
                }
            }
            added.addAndGet(input.batchSize);
        }
    }

    private Partition createPartitionIfAbsent(String simplePartitionName) throws Exception {

        NavigableMap<RingMember, RingHost> ring = amzaService.getRingReader().getRing("default".getBytes());
        if (ring.isEmpty()) {
            amzaService.getRingWriter().buildRandomSubRing("default".getBytes(), amzaService.getRingReader().getRingSize(AmzaRingReader.SYSTEM_RING));
        }

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
            null, 1000, 1000);

        PartitionName partitionName = new PartitionName(false, "default".getBytes(), simplePartitionName.getBytes());
        amzaService.setPropertiesIfAbsent(partitionName, new PartitionProperties(storageDescriptor, 2, false));

        long timeoutMillis = 10_000;
        while (true) {
            try {
                amzaService.awaitOnline(partitionName, timeoutMillis);
                break;
            } catch (TimeoutException te) {
                LOG.warn("{} failed to come online in {}", partitionName, timeoutMillis);
            }
        }
        return amzaService.getPartition(partitionName);
    }

    @Override
    public String getTitle() {
        return "Amza Stress";
    }

}
