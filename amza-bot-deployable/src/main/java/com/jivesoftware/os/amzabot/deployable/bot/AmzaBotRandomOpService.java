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

    public AmzaBotRandomOpService(AmzaBotRandomOpConfig config,
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

            String value = service.get(entry.getKey());

            if (value == null) {
                amzaKeyClearingHouse.quarantineEntry(entry, null);
                LOG.error("Did not find key {}:{}", entry.getKey());
            } else if (!entry.getValue().equals(value)) {
                amzaKeyClearingHouse.quarantineEntry(entry, value);
                LOG.error("Found key {}, but value differs. {} != {}",
                    entry.getKey(),
                    AmzaBotUtil.truncVal(entry.getValue()),
                    AmzaBotUtil.truncVal(value));
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

            synchronized (this) {
                try {
                    amzaKeyClearingHouse.delete(entry.getKey());
                    service.delete(entry.getKey());

                    LOG.debug("Deleted {}", entry.getKey());
                } catch (Exception e) {
                    amzaKeyClearingHouse.quarantineEntry(entry, "deleted");
                    LOG.error("Error deleting {}", entry.getKey(), e);
                }
            }
        } else {
            // write, odds are double read or delete
            if (amzaKeyClearingHouse.getKeyMap().size() < config.getWriteThreshold()) {
                Entry<String, String> entry =
                    amzaKeyClearingHouse.genRandomEntry(keySeed, config.getValueSizeThreshold());

                if (entry == null) {
                    LOG.error("No random entry was generated for {}", keySeed);
                } else {
                    synchronized (this) {
                        String oldValue = amzaKeyClearingHouse.set(entry.getKey(), entry.getValue());
                        if (oldValue != null) {
                            amzaKeyClearingHouse.quarantineEntry(
                                new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue()), oldValue);
                            LOG.error("Found existing kv pair: {}:{}", keySeed, oldValue);
                        }

                        service.set(keySeed, entry.getValue());
                    }
                }

                LOG.debug("Wrote {}:{}", keySeed, AmzaBotUtil.truncVal(entry.getValue()));
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
            ConcurrentMap<Integer, AtomicCounter> ops = Maps.newConcurrentMap();
            for (int i = 0; i < 4; i++) {
                ops.put(i, new AtomicCounter());
            }

            LOG.info("Executing random read/write/delete operations");

            while (running.get()) {
                try {
                    int op = randomOp(String.valueOf(seq.getValue()));
                    ops.get(op).inc();

                    if (config.getHesitationFactorMs() > 0) {
                        Thread.sleep(RANDOM.nextInt(config.getHesitationFactorMs()));
                    }

                    seq.inc();

                    if (seq.getValue() % 1_000L == 0) {
                        LOG.info("Executed {} random operations. {} reads, {} deletes, {} writes",
                            seq.getCount(),
                            ops.get(0).getValue(),
                            ops.get(1).getValue(),
                            ops.get(2).getValue() + ops.get(3).getValue());
                    }
                } catch (Exception e) {
                    LOG.error("Error occurred running random operation.", e);
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
