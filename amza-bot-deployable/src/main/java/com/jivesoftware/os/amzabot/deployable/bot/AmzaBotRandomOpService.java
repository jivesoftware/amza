package com.jivesoftware.os.amzabot.deployable.bot;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amzabot.deployable.AmzaBotService;
import com.jivesoftware.os.amzabot.deployable.AmzaBotUtil;
import com.jivesoftware.os.amzabot.deployable.AmzaKeyClearingHouse;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmzaBotRandomOpService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotRandomOpConfig config;
    private final AmzaBotService service;
    private final AmzaKeyClearingHouse amzaKeyClearingHouse;

    private Random RANDOM = new Random();
    private ExecutorService processor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("amzabot-randomops-%d").build());
    private final AtomicBoolean running = new AtomicBoolean();

    public AmzaBotRandomOpService(AmzaBotRandomOpConfig config,
        AmzaBotService service,
        AmzaKeyClearingHouse amzaKeyClearingHouse) {
        this.config = config;
        this.service = service;
        this.amzaKeyClearingHouse = amzaKeyClearingHouse;
    }

    public ConcurrentMap<String, Integer> getKeyMap() {
        return amzaKeyClearingHouse.getKeyMap();
    }

    public void clearKeyMap() {
        amzaKeyClearingHouse.clearKeyMap();
    }

    public ConcurrentMap<String, Entry<Integer, Integer>> getQuarantinedKeyMap() {
        return amzaKeyClearingHouse.getQuarantinedKeyMap();
    }

    public void clearQuarantinedKeyMap() {
        amzaKeyClearingHouse.clearQuarantinedKeyMap();
    }

    public int randomOp(String keySeed) throws Exception {
        int op = RANDOM.nextInt(7);

        // 0 - read
        // 1 - delete
        // 2 - batch write
        // 3 - batch delete
        // 4 - cas
        // 5,6 - write

        if (op == 0) {
            LOG.debug("random operation: read");

            Entry<String, Integer> entry = amzaKeyClearingHouse.getRandomEntry();
            if (entry == null) {
                LOG.debug("Need at least one entry to read");
                return op;
            }

            String value = service.getWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());

            if (value == null) {
                LOG.error("Did not find key {}", entry.getKey());

                amzaKeyClearingHouse.quarantineEntry(entry, null);
            } else if (entry.getValue() != value.hashCode()) {
                LOG.error("Found key {}, but value differs. {} != {}",
                    entry.getKey(),
                    entry.getValue(),
                    value.hashCode());

                amzaKeyClearingHouse.quarantineEntry(entry, value.hashCode());
                service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
            } else {
                LOG.debug("Found key {}", entry.getKey());
            }
        } else if (op == 1) {
            LOG.debug("random operation: delete");

            Entry<String, Integer> entry = amzaKeyClearingHouse.getRandomEntry();
            if (entry == null) {
                LOG.debug("Need at least one entry to delete");
                return op;
            }

            service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
            Integer oldValue = amzaKeyClearingHouse.delete(entry.getKey());
            if (oldValue == null) {
                LOG.error("Did not find existing kv pair: {}", entry.getKey());

                amzaKeyClearingHouse.quarantineEntry(
                    new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().hashCode()), null);
                service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
            }

            LOG.debug("Deleted {}", entry.getKey());
        } else if (op == 2) {
            LOG.debug("random operation: write batch; disregard write threshold");

            Set<Entry<String, String>> entries = Sets.newHashSet();
            for (int i = 0; i < RANDOM.nextInt(config.getBatchFactor()); i++) {
                entries.add(amzaKeyClearingHouse.genRandomEntry(keySeed + ":" + i, config.getValueSizeThreshold()));
            }

            service.multiSetWithInfiniteRetry(entries, config.getRetryWaitMs());

            for (Entry<String, String> entry : entries) {
                Integer oldValue = amzaKeyClearingHouse.set(entry.getKey(), entry.getValue());
                if (oldValue != null) {
                    LOG.error("Found existing kv pair: {}:{}", entry.getKey(), oldValue);

                    amzaKeyClearingHouse.quarantineEntry(
                        new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().hashCode()), oldValue);
                    service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
                }
            }

            LOG.debug("Wrote {} entries", entries.size());
        } else if (op == 3) {
            LOG.debug("random operation: delete batch; disregard write threshold");

            Set<Entry<String, Integer>> entries = Sets.newHashSet();
            for (int i = 0; i < RANDOM.nextInt(config.getBatchFactor()); i++) {
                Entry<String, Integer> entry = amzaKeyClearingHouse.getRandomEntry();
                if (entry == null) {
                    break;
                }

                entries.add(amzaKeyClearingHouse.getRandomEntry());
            }

            service.multiDeleteWithInfiniteRetry(entries, config.getRetryWaitMs());

            for (Entry<String, Integer> entry : entries) {
                Integer oldValue = amzaKeyClearingHouse.delete(entry.getKey());
                if (oldValue == null) {
                    LOG.error("Did not find existing kv pair: {}", entry.getKey());

                    amzaKeyClearingHouse.quarantineEntry(
                        new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().hashCode()), null);
                    service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
                }
            }

            LOG.debug("Deleted {} entries", entries.size());
        } else if (op == 4) {
            LOG.debug("random operation: cas");

            Entry<String, Integer> entry = amzaKeyClearingHouse.getRandomEntry();
            if (entry == null) {
                LOG.debug("Need at least one entry for cas");
                return op;
            }

            String oldPartitionValue = service.getWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());

            if (oldPartitionValue == null) {
                LOG.error("Did not find key {}", entry.getKey());

                amzaKeyClearingHouse.quarantineEntry(entry, null);
            } else if (oldPartitionValue.hashCode() != entry.getValue()) {
                LOG.error("Found key {}, but value differs. {} != {}",
                    entry.getKey(),
                    entry.getValue(),
                    AmzaBotUtil.truncVal(oldPartitionValue));

                amzaKeyClearingHouse.quarantineEntry(entry, oldPartitionValue.hashCode());
                service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
            } else {
                String newValue = amzaKeyClearingHouse.genRandomValue(config.getValueSizeThreshold());

                LOG.debug("Found key {}:{}. Set new value {}",
                    entry.getKey(), entry.getValue(), newValue.hashCode());

                service.setWithInfiniteRetry(entry.getKey(), newValue, config.getRetryWaitMs());

                Integer oldValue = amzaKeyClearingHouse.set(entry.getKey(), newValue);
                if (oldValue == null) {
                    LOG.error("Did not find key {}", entry.getKey());

                    amzaKeyClearingHouse.quarantineEntry(
                        new AbstractMap.SimpleEntry<>(entry.getKey(), newValue.hashCode()), null);
                    service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
                }

                LOG.debug("CAS {}:{}", entry.getKey(), AmzaBotUtil.truncVal(newValue));
            }
        } else {
            LOG.debug("random operation: write; odds are twice");

            if (amzaKeyClearingHouse.getKeyMap().size() < config.getWriteThreshold()) {
                Entry<String, String> entry =
                    amzaKeyClearingHouse.genRandomEntry(keySeed, config.getValueSizeThreshold());

                if (entry == null) {
                    LOG.error("No random entry was generated for {}", keySeed);
                } else {
                    service.setWithInfiniteRetry(entry.getKey(), entry.getValue(), config.getRetryWaitMs());

                    Integer oldValue = amzaKeyClearingHouse.set(entry.getKey(), entry.getValue());
                    if (oldValue != null) {
                        LOG.error("Found existing kv pair: {}:{}", entry.getKey(), oldValue);

                        amzaKeyClearingHouse.quarantineEntry(
                            new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue().hashCode()), oldValue);
                        service.deleteWithInfiniteRetry(entry.getKey(), config.getRetryWaitMs());
                    }

                    LOG.debug("Wrote {}:{}", entry.getKey(), AmzaBotUtil.truncVal(entry.getValue()));
                }
            } else {
                LOG.debug("Above write threshold of {} for {}.", config.getWriteThreshold(), keySeed);
            }
        }

        return op;
    }

    public void start() {
        LOG.info("Hesitation factor: {}", config.getHesitationFactorMs());
        LOG.info("Write threshold: {}", config.getWriteThreshold());
        LOG.info("Value size threshold: {}", config.getValueSizeThreshold());
        LOG.info("Durability: {}", config.getDurability());
        LOG.info("Consistency: {}", config.getConsistency());
        LOG.info("Ring size: {}", config.getRingSize());
        LOG.info("Retry wait: {}ms", config.getRetryWaitMs());
        LOG.info("Snapshot frequency: {}", config.getSnapshotFrequency());
        LOG.info("Client ordering: {}", config.getClientOrdering());
        LOG.info("Batch factor: {}", config.getBatchFactor());
        LOG.info("Tombstone timestamp age: {}ms", config.getTombstoneTimestampAgeInMillis());
        LOG.info("Tombstone timestamp interval: {}ms", config.getTombstoneTimestampIntervalMillis());

        if (!config.getEnabled()) {
            LOG.warn("Not starting random operations; not enabled.");
            return;
        }

        running.set(true);

        processor.submit(() -> {
            AtomicCounter seq = new AtomicCounter(ValueType.COUNT);

            ConcurrentMap<Integer, AtomicCounter> opsCounter = Maps.newConcurrentMap();
            for (int i = 0; i < 7; i++) {
                opsCounter.put(i, new AtomicCounter());
            }

            LOG.info("Executing random read/write/delete operations");

            while (running.get()) {
                try {
                    int op = randomOp(String.valueOf(seq.getValue()));
                    opsCounter.get(op).inc();
                    seq.inc();

                    if (config.getHesitationFactorMs() > 0) {
                        Thread.sleep(RANDOM.nextInt(config.getHesitationFactorMs()));
                    }

                    if (seq.getValue() % config.getSnapshotFrequency() == 0) {
                        LOG.info("Executed {} random operations. {} reads, {} deletes, {} batch writes, {} batch deletes, {} cas, {} writes",
                            seq.getCount(),
                            opsCounter.get(0).getValue(),
                            opsCounter.get(1).getValue(),
                            opsCounter.get(2).getValue(),
                            opsCounter.get(3).getValue(),
                            opsCounter.get(4).getValue(),
                            opsCounter.get(5).getValue() + opsCounter.get(6).getValue());

                        amzaKeyClearingHouse.verifyKeyMap(service.getAllWithInfiniteRetry(config.getRetryWaitMs()));
                    }
                } catch (Exception e) {
                    LOG.error("Error occurred running random operation. {}", e.getLocalizedMessage());
                    running.set(false);
                }
            }

            return null;
        });
    }

    public void stop() throws InterruptedException {
        running.set(false);
        Thread.sleep(config.getHesitationFactorMs());

        processor.shutdownNow();
    }

}
