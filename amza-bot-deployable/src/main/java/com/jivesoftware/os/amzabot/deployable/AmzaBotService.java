package com.jivesoftware.os.amzabot.deployable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.mlogger.core.AtomicCounter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.RandomStringUtils;

public class AmzaBotService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotConfig amzaBotConfig;
    private final PartitionClientProvider clientProvider;
    private final AmzaKeyClearingHouse amzaKeyClearingHouse;

    private Random rand = new Random();
    private ExecutorService processor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("amzabot-processor-%d").build());
    private final AtomicBoolean running = new AtomicBoolean();

    private final PartitionProperties AMZABOT_PROPERTIES = new PartitionProperties(
        Durability.fsync_async,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        false,
        Consistency.leader_quorum,
        true,
        true,
        false,
        RowType.snappy_primary,
        "lab",
        -1,
        null,
        -1,
        -1);

    private final PartitionName partitionName = new PartitionName(false,
        ("amzabot").getBytes(StandardCharsets.UTF_8),
        ("amzabot").getBytes(StandardCharsets.UTF_8));

    public AmzaBotService(AmzaBotConfig amzaBotConfig,
        PartitionClientProvider clientProvider,
        AmzaKeyClearingHouse amzaKeyClearingHouse) {
        this.amzaBotConfig = amzaBotConfig;
        this.clientProvider = clientProvider;
        this.amzaKeyClearingHouse = amzaKeyClearingHouse;
    }

    public void set(String k, String v) throws Exception {
        LOG.debug("set {}:{}", k, truncVal(v));

        if (amzaBotConfig.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping sets on the floor.");
            return;
        }

        PartitionClient partitionClient = clientProvider.getPartition(
            partitionName,
            amzaBotConfig.getPartitionSize(),
            AMZABOT_PROPERTIES);

        partitionClient.commit(
            Consistency.leader_quorum,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(k.getBytes(), v.getBytes(), -1, false);
                return true;
            },
            amzaBotConfig.getAdditionalSolverAfterNMillis(),
            amzaBotConfig.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public String get(String k) throws Exception {
        LOG.debug("get {}", k);

        PartitionClient partitionClient = clientProvider.getPartition(
            partitionName,
            3,
            AMZABOT_PROPERTIES);

        List<String> values = Lists.newArrayList();
        partitionClient.get(Consistency.leader_quorum,
            null,
            (keyStream) -> keyStream.stream(k.getBytes(StandardCharsets.UTF_8)),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    values.add(new String(value, StandardCharsets.UTF_8));
                }

                return true;
            },
            amzaBotConfig.getAbandonSolutionAfterNMillis(),
            amzaBotConfig.getAdditionalSolverAfterNMillis(),
            amzaBotConfig.getAbandonSolutionAfterNMillis(),
            Optional.empty());

        if (values.isEmpty()) {
            LOG.warn("key {} not found.", k);
            return null;
        }

        return Joiner.on(',').join(values);
    }

    public void delete(String k) throws Exception {
        LOG.debug("delete {}", k);

        PartitionClient partitionClient = clientProvider.getPartition(
            partitionName,
            3,
            AMZABOT_PROPERTIES);

        partitionClient.commit(
            Consistency.leader_quorum,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(k.getBytes(), null, -1, true);
                return true;
            },
            amzaBotConfig.getAdditionalSolverAfterNMillis(),
            amzaBotConfig.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public static String truncVal(String s) {
        int len = Math.min(s.length(), 10);
        String postfix = "";
        if (s.length() > len) {
            postfix = "...";
        }

        return s.substring(0, len) + postfix;
    }

    int randomOp(String keySeed) throws Exception {
        int op = rand.nextInt(4);

        if (op == 0) {
            // read
            Entry<String, String> entry = amzaKeyClearingHouse.getRandomKey();
            if (entry == null) {
                // need at least one write
                return op;
            }

            String value = get(entry.getKey());

            if (value == null) {
                amzaKeyClearingHouse.quarantineKey(entry, null);
                LOG.error("Did not find key {}:{}", entry.getKey());
            } else if (!entry.getValue().equals(value)) {
                amzaKeyClearingHouse.quarantineKey(entry, value);
                LOG.error("Found key {}, but value differs. {} != {}",
                    entry.getKey(),
                    truncVal(entry.getValue()),
                    truncVal(value));
            } else {
                LOG.debug("Found key {}", entry.getKey());
            }
        } else if (op == 1) {
            // delete
            Entry<String, String> entry = amzaKeyClearingHouse.getRandomKey();
            if (entry == null) {
                // need at least one write
                return op;
            }

            synchronized (this) {
                try {
                    amzaKeyClearingHouse.delete(entry.getKey());
                    delete(entry.getKey());

                    LOG.debug("Deleted {}", entry.getKey());
                } catch (Exception e) {
                    amzaKeyClearingHouse.quarantineKey(entry, "deleted");
                    LOG.error("Error deleting {}", entry.getKey(), e);
                }
            }
        } else {
            // write, odds are double read or delete
            if (amzaKeyClearingHouse.getKeyMap().size() < amzaBotConfig.getWriteThreshold()) {
                if (amzaBotConfig.getDropEverythingOnTheFloor()) {
                    LOG.warn("Dropping random write operations on the floor.");
                } else {
                    String value = RandomStringUtils.randomAlphanumeric(
                        rand.nextInt(amzaBotConfig.getValueSizeThreshold()));

                    synchronized (this) {
                        String oldValue = amzaKeyClearingHouse.set(keySeed, value);
                        if (oldValue != null) {
                            amzaKeyClearingHouse.quarantineKey(new AbstractMap.SimpleEntry<>(keySeed, value), oldValue);
                            LOG.error("Found existing kv pair: {}:{}", keySeed, oldValue);
                        }

                        set(keySeed, value);
                    }

                    LOG.debug("Wrote {}:{}", keySeed, truncVal(value));
                }
            } else {
                LOG.debug("Above write threshold of {} for {}.", amzaBotConfig.getWriteThreshold(), keySeed);
            }
        }

        return op;
    }

    void start() {
        running.set(true);

        processor.submit(() -> {
            AtomicCounter seq = new AtomicCounter(ValueType.COUNT);

            while (running.get()) {
                randomOp(String.valueOf(seq.getValue()));
                Thread.sleep(rand.nextInt(amzaBotConfig.getHesitationFactorMs()));

                seq.inc();
            }

            return null;
        });
    }

    void stop() throws InterruptedException {
        running.set(false);
        Thread.sleep(amzaBotConfig.getHesitationFactorMs());

        processor.shutdownNow();
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

}
