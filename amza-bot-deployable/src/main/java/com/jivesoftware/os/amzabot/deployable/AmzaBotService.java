package com.jivesoftware.os.amzabot.deployable;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.Consistency;
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
    private final PartitionClient client;
    private final Consistency consistency;

    public AmzaBotService(AmzaBotConfig config,
        PartitionClient client,
        Consistency consistency) {
        this.config = config;
        this.client = client;
        this.consistency = consistency;
    }

    public void set(String k, String v) throws Exception {
        LOG.debug("set {}:{}", k, AmzaBotUtil.truncVal(v));

        if (config.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping sets on the floor.");
            return;
        }

        client.commit(
            consistency,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(k.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8), -1, false);
                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public void set(Set<Entry<String, String>> entries) throws Exception {
        if (entries == null) {
            LOG.warn("Empty set of entries.");
            return;
        }

        for (Entry<String, String> entry : entries) {
            LOG.debug("set {}:{}", entry.getKey(), AmzaBotUtil.truncVal(entry.getValue()));
        }

        if (config.getDropEverythingOnTheFloor()) {
            LOG.warn("Dropping sets on the floor.");
            return;
        }

        client.commit(
            consistency,
            null,
            (commitKeyValueStream) -> {
                for (Entry<String, String> entry : entries) {
                    commitKeyValueStream.commit(
                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                        entry.getValue().getBytes(StandardCharsets.UTF_8),
                        -1,
                        false);
                }
                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());
    }

    public String get(String k) throws Exception {
        LOG.debug("get {}", k);

        List<String> values = Lists.newArrayList();
        client.get(consistency,
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

    public String delete(String k) throws Exception {
        LOG.debug("delete {}", k);

        String res = get(k);

        client.commit(consistency,
            null,
            (commitKeyValueStream) -> {
                commitKeyValueStream.commit(k.getBytes(StandardCharsets.UTF_8), null, -1, true);
                return true;
            },
            config.getAdditionalSolverAfterNMillis(),
            config.getAbandonSolutionAfterNMillis(),
            Optional.empty());

        return res;
    }

    public Map<String, String> getAll() throws Exception {
        ConcurrentMap<String, String> res = Maps.newConcurrentMap();

        client.scan(consistency,
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
