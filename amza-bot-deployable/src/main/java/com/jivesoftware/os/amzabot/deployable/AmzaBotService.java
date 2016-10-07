package com.jivesoftware.os.amzabot.deployable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class AmzaBotService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaBotConfig config;
    private final PartitionClientProvider partitionClientProvider;
    private final OrderIdProvider orderIdProvider;
    private final Durability durability;
    private final Consistency consistency;
    private final String partitionNameName;
    private final int ringSize;

    private PartitionClient partitionClient;

    public AmzaBotService(AmzaBotConfig config,
        PartitionClientProvider partitionClientProvider,
        OrderIdProvider orderIdProvider,
        Durability durability,
        Consistency consistency,
        String partitionNameName,
        int ringSize) {
        this.config = config;
        this.partitionClientProvider = partitionClientProvider;
        this.orderIdProvider = orderIdProvider;
        this.durability = durability;
        this.consistency = consistency;
        this.partitionNameName = partitionNameName;
        this.ringSize = ringSize;
    }

    private void initialize() throws Exception {
        PartitionProperties partitionProperties = new PartitionProperties(
            durability,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            consistency,
            true,
            true,
            false,
            RowType.snappy_primary,
            "lab",
            -1,
            null,
            -1,
            -1);

        PartitionName partitionName = new PartitionName(false,
            ("amzabot").getBytes(StandardCharsets.UTF_8),
            partitionNameName.getBytes(StandardCharsets.UTF_8));

        partitionClient = partitionClientProvider.getPartition(
            partitionName,
            ringSize,
            partitionProperties);

        LOG.info("Created partition for amzabot {} of size {}", partitionName, ringSize);
    }

    public void setWithInfiniteRetry(String k, String v, int retryIntervalMs) throws Exception {
        setWithRetry(k, v, Integer.MAX_VALUE - 1, retryIntervalMs);
    }

    void setWithRetry(String k, String v,
        int retryCount, int retryIntervalMs) throws Exception {
        int retriesLeft = retryCount;
        if (retriesLeft < Integer.MAX_VALUE) {
            retriesLeft++;
        }

        int currentTryCount = 1;

        while (retriesLeft > 0) {
            try {
                set(k, v);
                retriesLeft = 0;
            } catch (Exception e) {
                LOG.error("Error occurred setting key {}:{} - {}",
                    k,
                    AmzaBotUtil.truncVal(v),
                    e.getLocalizedMessage());

                if (retriesLeft > 0) {
                    if (retryIntervalMs > 0) {
                        LOG.info("Retry setting in {}ms. Tried {} times.", retryIntervalMs, currentTryCount);
                        Thread.sleep(retryIntervalMs);
                    } else {
                        LOG.info("Retry setting value. Tried {} times.", currentTryCount);
                    }
                }
            } finally {
                retriesLeft--;
                currentTryCount++;
            }
        }
    }

    public void set(String k, String v) throws Exception {
        LOG.debug("set {}:{}", k, AmzaBotUtil.truncVal(v));

        if (partitionClient == null) {
            initialize();
        }

        if (config.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping sets on the floor.");
            return;
        }

        partitionClient.commit(
            consistency,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(
                    k.getBytes(StandardCharsets.UTF_8),
                    v.getBytes(StandardCharsets.UTF_8),
                    orderIdProvider.nextId(),
                    false);

                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public String getWithInfiniteRetry(String k, int retryIntervalMs) throws Exception {
        return getWithRetry(k, Integer.MAX_VALUE - 1, retryIntervalMs);
    }

    String getWithRetry(String k,
        int retryCount, int retryIntervalMs) throws Exception {
        int retriesLeft = retryCount;
        if (retriesLeft < Integer.MAX_VALUE) {
            retriesLeft++;
        }

        int currentTryCount = 1;

        String res = "";

        while (retriesLeft > 0) {
            try {
                res = get(k);
                retriesLeft = 0;
            } catch (Exception e) {
                LOG.error("Error occurred getting key {} - {}",
                    k,
                    e.getLocalizedMessage());

                if (retriesLeft > 0) {
                    if (retryIntervalMs > 0) {
                        LOG.info("Retry getting in {}ms. Tried {} times.", retryIntervalMs, currentTryCount);
                        Thread.sleep(retryIntervalMs);
                    } else {
                        LOG.info("Retry getting value. Tried {} times.", currentTryCount);
                    }
                }
            } finally {
                retriesLeft--;
                currentTryCount++;
            }
        }

        return res;
    }

    public String get(String k) throws Exception {
        LOG.debug("get {}", k);

        if (partitionClient == null) {
            initialize();
        }

        List<String> values = Lists.newArrayList();
        partitionClient.get(consistency,
            null,
            (keyStream) -> keyStream.stream(k.getBytes(StandardCharsets.UTF_8)),
            (prefix, key, value, timestamp, version) -> {
                if (value != null) {
                    values.add(new String(value, StandardCharsets.UTF_8));
                }

                return true;
            },
            config.getAbandonSolutionAfterNMillis(),
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());

        if (values.isEmpty()) {
            LOG.warn("key {} not found.", k);
            return null;
        }

        return Joiner.on(',').join(values);
    }

    public String deleteWithInfiniteRetry(String k, int retryIntervalMs) throws Exception {
        return deleteWithRetry(k, Integer.MAX_VALUE - 1, retryIntervalMs);
    }

    String deleteWithRetry(String k,
        int retryCount, int retryIntervalMs) throws Exception {
        int retriesLeft = retryCount;
        if (retriesLeft < Integer.MAX_VALUE) {
            retriesLeft++;
        }

        int currentTryCount = 1;

        String res = "";

        while (retriesLeft > 0) {
            try {
                res = delete(k);
                retriesLeft = 0;
            } catch (Exception e) {
                LOG.error("Error occurred deleting key {} - {}",
                    k,
                    e.getLocalizedMessage());

                if (retriesLeft > 0) {
                    if (retryIntervalMs > 0) {
                        LOG.info("Retry deleting in {}ms. Tried {} times.", retryIntervalMs, currentTryCount);
                        Thread.sleep(retryIntervalMs);
                    } else {
                        LOG.info("Retry deleting value. Tried {} times.", currentTryCount);
                    }
                }
            } finally {
                retriesLeft--;
                currentTryCount++;
            }
        }

        return res;
    }

    public String delete(String k) throws Exception {
        LOG.debug("delete {}", k);

        if (partitionClient == null) {
            initialize();
        }

        String res = get(k);

        if (res != null) {
            partitionClient.commit(
                consistency,
                null,
                (commitKeyValueStream) -> {
                    commitKeyValueStream.commit(
                        k.getBytes(StandardCharsets.UTF_8),
                        null,
                        orderIdProvider.nextId(),
                        true);

                    return true;
                },
                config.getAdditionalSolverAfterNMillis(),
                config.getAbandonSolutionAfterNMillis(),
                Optional.empty());
        }

        return res;
    }

    public void multiSetWithInfiniteRetry(Set<Entry<String, String>> entries, int retryIntervalMs) throws Exception {
        multiSetWithRetry(entries, Integer.MAX_VALUE - 1, retryIntervalMs);
    }

    public void multiSetWithRetry(Set<Entry<String, String>> entries,
        int retryCount, int retryIntervalMs) throws Exception {
        int retriesLeft = retryCount;
        if (retriesLeft < Integer.MAX_VALUE) {
            retriesLeft++;
        }

        int currentTryCount = 1;

        while (retriesLeft > 0) {
            try {
                multiSet(entries);
                retriesLeft = 0;
            } catch (Exception e) {
                LOG.error("Error occurred multi-setting {} keys - {}",
                    entries.size(),
                    e.getLocalizedMessage());

                if (retriesLeft > 0) {
                    if (retryIntervalMs > 0) {
                        LOG.info("Retry batch write in {}ms. Tried {} times.", retryIntervalMs, currentTryCount);
                        Thread.sleep(retryIntervalMs);
                    } else {
                        LOG.info("Retry batch write. Tried {} times.", currentTryCount);
                    }
                }
            } finally {
                retriesLeft--;
                currentTryCount++;
            }
        }
    }

    public void multiSet(Set<Entry<String, String>> entries) throws Exception {
        if (entries == null) {
            LOG.warn("Empty set of entries.");
            return;
        }

        if (partitionClient == null) {
            initialize();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Multi-set {}:{}", entries.size(), Joiner.on(",").join(entries));
        }

        if (config.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping multi-sets on the floor.");
            return;
        }

        partitionClient.commit(
            consistency,
            null,
            (commitKeyValueStream) -> {
                for (Entry<String, String> entry : entries) {
                    commitKeyValueStream.commit(
                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                        entry.getValue().getBytes(StandardCharsets.UTF_8),
                        orderIdProvider.nextId(),
                        false);
                }

                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public void multiDeleteWithInfiniteRetry(Set<Entry<String, Integer>> entries, int retryIntervalMs) throws Exception {
        multiDeleteWithRetry(entries, Integer.MAX_VALUE - 1, retryIntervalMs);
    }

    public void multiDeleteWithRetry(Set<Entry<String, Integer>> entries,
        int retryCount, int retryIntervalMs) throws Exception {
        int retriesLeft = retryCount;
        if (retriesLeft < Integer.MAX_VALUE) {
            retriesLeft++;
        }

        int currentTryCount = 1;

        while (retriesLeft > 0) {
            try {
                multiDelete(entries);
                retriesLeft = 0;
            } catch (Exception e) {
                LOG.error("Error occurred multi-deleting {} keys - {}",
                    entries.size(),
                    e.getLocalizedMessage());

                if (retriesLeft > 0) {
                    if (retryIntervalMs > 0) {
                        LOG.info("Retry batch delete in {}ms. Tried {} times.", retryIntervalMs, currentTryCount);
                        Thread.sleep(retryIntervalMs);
                    } else {
                        LOG.info("Retry batch delete. Tried {} times.", currentTryCount);
                    }
                }
            } finally {
                retriesLeft--;
                currentTryCount++;
            }
        }
    }

    public void multiDelete(Set<Entry<String, Integer>> entries) throws Exception {
        if (entries == null) {
            LOG.warn("Empty set of entries.");
            return;
        }

        if (partitionClient == null) {
            initialize();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Multi-delete {}:{}", entries.size(), Joiner.on(",").join(entries));
        }

        if (config.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping multi-deletes on the floor.");
            return;
        }

        partitionClient.commit(
            consistency,
            null,
            (commitKeyValueStream) -> {
                for (Entry<String, Integer> entry : entries) {
                    commitKeyValueStream.commit(
                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                        null,
                        orderIdProvider.nextId(),
                        true);
                }

                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public Map<String, String> getAllWithInfiniteRetry(
        int retryIntervalMs) throws Exception {
        return getAllWithRetry(Integer.MAX_VALUE - 1, retryIntervalMs);
    }

    Map<String, String> getAllWithRetry(
        int retryCount, int retryIntervalMs) throws Exception {
        int retriesLeft = retryCount;
        if (retriesLeft < Integer.MAX_VALUE) {
            retriesLeft++;
        }

        int currentTryCount = 1;

        Map<String, String> res = Maps.newConcurrentMap();

        while (retriesLeft > 0) {
            try {
                res = getAll();
                retriesLeft = 0;
            } catch (Exception e) {
                LOG.error("Error occurred getting all keys {}",
                    e.getLocalizedMessage());

                if (retriesLeft > 0) {
                    if (retryIntervalMs > 0) {
                        LOG.info("Retry getting all values in {}ms. Tried {} times.", retryIntervalMs, currentTryCount);
                        Thread.sleep(retryIntervalMs);
                    } else {
                        LOG.info("Retry getting all values. Tried {} times.", currentTryCount);
                    }
                }
            } finally {
                retriesLeft--;
                currentTryCount++;
            }
        }

        return res;
    }

    public Map<String, String> getAll() throws Exception {
        ConcurrentMap<String, String> res = Maps.newConcurrentMap();

        if (partitionClient == null) {
            initialize();
        }

        partitionClient.scan(consistency,
            false,
            stream -> stream.stream(null, null, null, null),
            (prefix, key, value, timestamp, version) -> {
                res.put(new String(key, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8));
                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonLeaderSolutionAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());

        return res;
    }

}
