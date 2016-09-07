package com.jivesoftware.os.amzabot.deployable.bot;

import com.google.common.collect.Maps;
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
        new ThreadFactoryBuilder().setNameFormat("amzabot-processor-%d").build());
    private final AtomicBoolean running = new AtomicBoolean();

    AmzaBotRandomOpService(AmzaBotRandomOpConfig config,
        AmzaBotService service,
        AmzaKeyClearingHouse amzaKeyClearingHouse) {
        this.config = config;
        this.service = service;
        this.amzaKeyClearingHouse = amzaKeyClearingHouse;
    }

    public ConcurrentMap<String, String> getKeyMap() {
        return amzaKeyClearingHouse.getKeyMap();
    }

    public void clearKeyMap() {
        amzaKeyClearingHouse.clearKeyMap();
    }

    public ConcurrentMap<String, Entry<String, String>> getQuarantinedKeyMap() {
        return amzaKeyClearingHouse.getQuarantinedKeyMap();
    }

    public void clearQuarantinedKeyMap() {
        amzaKeyClearingHouse.clearQuarantinedKeyMap();
    }

    public int randomOp(String keySeed) throws Exception {
        int op = RANDOM.nextInt(4);

        if (op == 0) {
            // read
            Entry<String, String> entry = amzaKeyClearingHouse.getRandomEntry();
            if (entry == null) {
                // need at least one write
                return op;
            }

            String value = service.getWithRetry(entry.getKey(), Integer.MAX_VALUE, config.getRetryWaitMs());

            if (value == null) {
                LOG.error("Did not find key {}", entry.getKey());

                amzaKeyClearingHouse.quarantineEntry(entry, null);
            } else if (!entry.getValue().equals(value)) {
                LOG.error("Found key {}, but value differs. {} != {}",
                    entry.getKey(),
                    AmzaBotUtil.truncVal(entry.getValue()),
                    AmzaBotUtil.truncVal(value));

                amzaKeyClearingHouse.quarantineEntry(entry, value);
                service.deleteWithRetry(entry.getKey(), Integer.MAX_VALUE, config.getRetryWaitMs());
            } else {
                LOG.debug("Found key {}", entry.getKey());
            }
        } else if (op == 1) {
            // delete
            Entry<String, String> entry = amzaKeyClearingHouse.getRandomEntry();
            if (entry == null) {
                // need at least one write
                return op;
            }

            service.deleteWithRetry(entry.getKey(), 0, config.getRetryWaitMs());
            amzaKeyClearingHouse.delete(entry.getKey());

            LOG.debug("Deleted {}", entry.getKey());
        } else {
            // write, odds are double that of read or delete
            if (amzaKeyClearingHouse.getKeyMap().size() < config.getWriteThreshold()) {
                Entry<String, String> entry =
                    amzaKeyClearingHouse.genRandomEntry(keySeed, config.getValueSizeThreshold());

                if (entry == null) {
                    LOG.error("No random entry was generated for {}", keySeed);
                } else {
                    service.setWithRetry(entry.getKey(), entry.getValue(),
                        Integer.MAX_VALUE, config.getRetryWaitMs());

                    String oldValue = amzaKeyClearingHouse.set(entry.getKey(), entry.getValue());
                    if (oldValue != null) {
                        LOG.error("Found existing kv pair: {}:{}", entry.getKey(), oldValue);

                        amzaKeyClearingHouse.quarantineEntry(
                            new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()), oldValue);
                        service.deleteWithRetry(entry.getKey(), Integer.MAX_VALUE, config.getRetryWaitMs());
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
        if (!config.getEnabled()) {
            LOG.warn("Not starting random operations; not enabled.");
            return;
        }

        running.set(true);

        processor.submit(() -> {
            AtomicCounter seq = new AtomicCounter(ValueType.COUNT);

            ConcurrentMap<Integer, AtomicCounter> opsCounter = Maps.newConcurrentMap();
            for (int i = 0; i < 4; i++) {
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
                        LOG.info("Executed {} random operations. {} reads, {} deletes, {} writes",
                            seq.getCount(),
                            opsCounter.get(0).getValue(),
                            opsCounter.get(1).getValue(),
                            opsCounter.get(2).getValue() + opsCounter.get(3).getValue());

                        amzaKeyClearingHouse.verifyKeyMap(service.getAll());
                    }
                } catch (Exception e) {
                    LOG.error("Error occurred running random operation.", e.getLocalizedMessage());
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
