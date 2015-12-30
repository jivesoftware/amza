package com.jivesoftware.os.amza.ui.region;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.api.partition.WALStorageDescriptor;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.http.exceptions.NotSolveableException;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedPartitionClient;
import com.jivesoftware.os.amza.ui.soy.SoyRenderer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final PartitionClientProvider partitionClientProvider;

    public AmzaStressPluginRegion(String template,
        SoyRenderer renderer,
        AmzaService amzaService,
        PartitionClientProvider partitionClientProvider) {
        this.template = template;
        this.renderer = renderer;
        this.amzaService = amzaService;
        this.partitionClientProvider = partitionClientProvider;
    }

    public static class AmzaStressPluginRegionInput {

        final String name;
        final boolean client;
        final String indexClassName;
        final int maxUpdatesBetweenCompactionHintMarker;
        final int maxUpdatesBetweenIndexCommitMarker;
        final String ringName;
        final String regionPrefix;
        final int ringSize;
        final int numBatches;
        final int batchSize;
        final int numPartitions;
        final int numThreadsPerPartition;
        final int numKeyPrefixes;
        final String consistency;
        final boolean requireConsistency;
        final boolean orderedInsertion;
        final String action;
        final RowType rowType;

        public AmzaStressPluginRegionInput(String name,
            boolean client,
            String indexClassName,
            int maxUpdatesBetweenCompactionHintMarker,
            int maxUpdatesBetweenIndexCommitMarker,
            String ringName,
            String regionPrefix,
            int ringSize,
            int numBatches,
            int batchSize,
            int numPartitions,
            int numThreadsPerRegion,
            int numKeyPrefixes,
            String consistency,
            boolean requireConsistency,
            boolean orderedInsertion,
            String action,
            RowType rowType) {
            this.name = name;
            this.client = client;
            this.indexClassName = indexClassName;
            this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
            this.maxUpdatesBetweenIndexCommitMarker = maxUpdatesBetweenIndexCommitMarker;
            this.ringName = ringName;
            this.regionPrefix = regionPrefix;
            this.ringSize = ringSize;
            this.numBatches = numBatches;
            this.batchSize = batchSize;
            this.numPartitions = numPartitions;
            this.numThreadsPerPartition = numThreadsPerRegion;
            this.numKeyPrefixes = numKeyPrefixes;
            this.consistency = consistency;
            this.requireConsistency = requireConsistency;
            this.orderedInsertion = orderedInsertion;
            this.action = action;
            this.rowType = rowType;
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
                    ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10,
                        new ThreadFactoryBuilder().setNameFormat("stress-%d").build());
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
                row.put("client", String.valueOf(stress.input.client));
                row.put("indexClassName", stress.input.indexClassName);
                row.put("maxUpdatesBetweenCompactionHintMarker", String.valueOf(stress.input.maxUpdatesBetweenCompactionHintMarker));
                row.put("maxUpdatesBetweenIndexCommitMarker", String.valueOf(stress.input.maxUpdatesBetweenIndexCommitMarker));
                row.put("ringName", stress.input.ringName);
                row.put("regionPrefix", stress.input.regionPrefix);
                row.put("numBatches", String.valueOf(stress.input.numBatches));
                row.put("batchSize", String.valueOf(stress.input.batchSize));
                row.put("numPartitions", String.valueOf(stress.input.numPartitions));
                row.put("numThreadsPerRegion", String.valueOf(stress.input.numThreadsPerPartition));
                row.put("numKeyPrefixes", String.valueOf(stress.input.numKeyPrefixes));
                row.put("consistency", String.valueOf(stress.input.consistency));
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
                    executor.submit(new Feeder(input.client,
                        input.indexClassName,
                        input.maxUpdatesBetweenCompactionHintMarker,
                        input.maxUpdatesBetweenIndexCommitMarker,
                        input.ringName,
                        regionName,
                        input.ringSize,
                        Consistency.valueOf(input.consistency),
                        input.requireConsistency,
                        j,
                        input.orderedInsertion,
                        input.rowType));
                }
            }
        }

        private class Feeder implements Runnable {

            AtomicInteger batch = new AtomicInteger();
            private final boolean client;
            private final String indexClassName;
            private final int maxUpdatesBetweenCompactionHintMarker;
            private final int maxUpdatesBetweenIndexCommitMarker;
            private final String ringName;
            private final String regionName;
            private final int ringSize;
            private final Consistency consistency;
            private final boolean requireConsistency;
            private final int threadIndex;
            private final boolean orderedInsertion;
            private final RowType rowType;

            public Feeder(boolean client,
                String indexClassName,
                int maxUpdatesBetweenCompactionHintMarker,
                int maxUpdatesBetweenIndexCommitMarker,
                String ringName,
                String regionName,
                int ringSize,
                Consistency consistency,
                boolean requireConsistency,
                int threadIndex,
                boolean orderedInsertion,
                RowType rowType) {
                this.client = client;
                this.indexClassName = indexClassName;
                this.maxUpdatesBetweenCompactionHintMarker = maxUpdatesBetweenCompactionHintMarker;
                this.maxUpdatesBetweenIndexCommitMarker = maxUpdatesBetweenIndexCommitMarker;
                this.ringName = ringName;
                this.regionName = regionName;
                this.ringSize = ringSize;
                this.consistency = consistency;
                this.requireConsistency = requireConsistency;
                this.threadIndex = threadIndex;
                this.orderedInsertion = orderedInsertion;
                this.rowType = rowType;
            }

            @Override
            public void run() {
                try {
                    int b = batch.incrementAndGet();
                    if (b <= input.numBatches && !forcedStop.get()) {
                        feed(client, indexClassName, maxUpdatesBetweenCompactionHintMarker, maxUpdatesBetweenIndexCommitMarker,
                            ringName, regionName, ringSize, consistency, requireConsistency, b, threadIndex, rowType);
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

        private void feed(boolean client,
            String indexClassName,
            int maxUpdatesBetweenCompactionHintMarker,
            int maxUpdatesBetweenIndexCommitMarker,
            String ringName,
            String regionName,
            int ringSize,
            Consistency consistency,
            boolean requireConsistency,
            int batch,
            int threadIndex,
            RowType rowType) throws Exception {
            PartitionClient partition = createPartitionIfAbsent(client,
                indexClassName,
                maxUpdatesBetweenCompactionHintMarker,
                maxUpdatesBetweenIndexCommitMarker,
                ringName,
                regionName,
                consistency,
                requireConsistency,
                ringSize,
                rowType);

            while (true) {
                try {
                    byte[] prefix = input.numKeyPrefixes > 0 ? UIO.intBytes(batch % input.numKeyPrefixes) : null;
                    partition.commit(consistency, prefix,
                        (txKeyValueStream) -> {
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
                        },
                        30_000,
                        60_000,
                        Optional.<List<String>>empty());
                    break;

                } catch (DeltaOverCapacityException de) {
                    Thread.sleep(100);
                    LOG.warn("Delta over capacity for region:{} batch:{} thread:{}", new Object[]{regionName, batch, threadIndex});
                } catch (NotSolveableException e) {
                    Thread.sleep(100);
                    LOG.warn("Not solveable for region:{} batch:{} thread:{}", new Object[]{regionName, batch, threadIndex});
                } catch (Exception x) {
                    LOG.warn("Failed to set region:{} batch:{} thread:{}", new Object[]{regionName, batch, threadIndex}, x);
                }
            }
            added.addAndGet(input.batchSize);
        }
    }

    private PartitionClient createPartitionIfAbsent(boolean client,
        String indexClassName,
        int maxUpdatesBetweenCompactionHintMarker,
        int maxUpdatesBetweenIndexCommitMarker,
        String ringName,
        String simplePartitionName,
        Consistency consistency,
        boolean requireConsistency,
        int ringSize,
        RowType rowType) throws Exception {

        if (ringSize < 1) {
            amzaService.getRingWriter().ensureMaximalRing(ringName.getBytes());
        } else {
            amzaService.getRingWriter().ensureSubRing(ringName.getBytes(), ringSize);

        }

        WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(false,
            new PrimaryIndexDescriptor(indexClassName, 0, false, null),
            null, maxUpdatesBetweenCompactionHintMarker, maxUpdatesBetweenIndexCommitMarker);
        PartitionProperties partitionProperties = new PartitionProperties(storageDescriptor, consistency, requireConsistency, 2, false, rowType);
        long timeoutMillis = 10_000;
        PartitionName partitionName = new PartitionName(false, ringName.getBytes(), simplePartitionName.getBytes());

        if (client) {
            while (true) {
                try {
                    return partitionClientProvider.getPartition(partitionName, ringSize, partitionProperties);
                } catch (Exception x) {
                    LOG.warn("{} failed to come online in {}", partitionName, timeoutMillis);
                }
            }
        } else {
            amzaService.setPropertiesIfAbsent(partitionName, partitionProperties);

            while (true) {
                try {
                    amzaService.awaitOnline(partitionName, timeoutMillis);
                    amzaService.awaitLeader(partitionName, timeoutMillis);
                    return new EmbeddedPartitionClient(amzaService.getPartition(partitionName), amzaService.getRingReader().getRingMember());
                } catch (TimeoutException te) {
                    LOG.warn("{} failed to come online in {}", partitionName, timeoutMillis);
                }
            }
        }
    }

    @Override
    public String getTitle() {
        return "Amza Stress";
    }

}
