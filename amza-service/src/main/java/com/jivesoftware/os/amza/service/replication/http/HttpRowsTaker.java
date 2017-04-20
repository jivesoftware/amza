/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.service.replication.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.take.RowsTaker;
import com.jivesoftware.os.amza.service.take.StreamingTakesConsumer;
import com.jivesoftware.os.amza.service.take.StreamingTakesConsumer.StreamingTakeConsumed;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.nustaq.serialization.FSTConfiguration;
import org.xerial.snappy.SnappyInputStream;

import static sun.swing.SwingUtilities2.submit;

public class HttpRowsTaker implements RowsTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final String name;
    private final AmzaStats amzaStats;
    private final TenantAwareHttpClient<String> ringClient;
    private final ObjectMapper mapper;
    private final StreamingTakesConsumer streamingTakesConsumer;
    private final ExecutorService flushExecutor;

    private final AtomicLong flushVersion = new AtomicLong();
    private final Map<RingHost, Ackable> hostQueue = Maps.newConcurrentMap();

    public HttpRowsTaker(String name,
        AmzaStats amzaStats,
        TenantAwareHttpClient<String> ringClient,
        ObjectMapper mapper,
        AmzaInterner amzaInterner,
        ExecutorService queueExecutor,
        ExecutorService flushExecutor) {
        this.name = name;
        this.amzaStats = amzaStats;
        this.ringClient = ringClient;
        this.mapper = mapper;
        this.streamingTakesConsumer = new StreamingTakesConsumer(amzaInterner);
        this.flushExecutor = flushExecutor;

        //TODO lifecycle
        queueExecutor.submit(() -> {
            while (true) {
                try {
                    long currentVersion = flushVersion.get();
                    for (Entry<RingHost, Ackable> entry : hostQueue.entrySet()) {
                        Ackable ackable = entry.getValue();
                        if (ackable.running.compareAndSet(false, true)) {
                            flushQueues(entry.getKey(), ackable, currentVersion);
                        }
                    }
                    synchronized (flushVersion) {
                        if (currentVersion == flushVersion.get()) {
                            flushVersion.wait();
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("HttpRowsTaker failure", t);
                }
            }
        });
    }

    /**
     * @param localRingMember
     * @param remoteRingMember
     * @param remoteRingHost
     * @param remoteVersionedPartitionName
     * @param remoteTxId
     * @param rowStream
     * @return Will return null if the other node was reachable but the partition on that node was NOT online.
     * @throws Exception
     */
    @Override
    public StreamingRowsResult rowsStream(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        VersionedPartitionName remoteVersionedPartitionName,
        long takeSessionId,
        String takeSharedKey,
        long remoteTxId,
        long localLeadershipToken,
        long limit,
        RowStream rowStream) {

        HttpStreamResponse httpStreamResponse;
        try {
            String endpoint = "/amza/rows/stream/" + localRingMember.getMember()
                + "/" + remoteVersionedPartitionName.toBase64()
                + "/" + takeSessionId
                + "/" + remoteTxId
                + "/" + localLeadershipToken
                + "/" + limit;
            String sharedKeyJson = mapper.writeValueAsString(takeSharedKey);
            httpStreamResponse = ringClient.call("",
                new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(remoteRingHost.getHost(), remoteRingHost.getPort()) }),
                "rowsStream",
                httpClient -> {
                    HttpStreamResponse response = httpClient.streamingPost(endpoint, sharedKeyJson, null);
                    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                        throw new NonSuccessStatusCodeException(response.getStatusCode(), response.getStatusReasonPhrase());
                    }
                    return new ClientResponse<>(response, true);
                });
        } catch (IOException | HttpClientException e) {
            return new StreamingRowsResult(e, null, -1, -1, null);
        }
        try {
            BufferedInputStream bis = new BufferedInputStream(httpStreamResponse.getInputStream(), 8192); // TODO config??
            DataInputStream dis = new DataInputStream(new SnappyInputStream(bis));
            StreamingTakeConsumed consumed = streamingTakesConsumer.consume(dis, rowStream);
            amzaStats.netStats.read.add(consumed.bytes);
            Map<RingMember, Long> otherHighwaterMarks = (consumed.streamedToEnd && consumed.isOnline) ? consumed.neighborsHighwaterMarks : null;
            return new StreamingRowsResult(null, null, consumed.leadershipToken, consumed.partitionVersion, otherHighwaterMarks);
        } catch (Exception e) {
            return new StreamingRowsResult(null, e, -1, -1, null);
        } finally {
            httpStreamResponse.close();
        }
    }

    private static class Ackable {
        public final AtomicBoolean running = new AtomicBoolean(false);
        public final Semaphore semaphore = new Semaphore(Short.MAX_VALUE);
        public final AtomicReference<Map<VersionedPartitionName, RowsTakenPayload>> rowsTakenPayloads = new AtomicReference<>(Maps.newConcurrentMap());
        public final AtomicReference<PongPayload> pongPayloads = new AtomicReference<>();
    }

    public static class RowsTakenAndPong implements Serializable {
        public final Map<VersionedPartitionName, RowsTakenPayload> rowsTakenPayloads;
        public final PongPayload pongPayload;

        @JsonCreator
        public RowsTakenAndPong(
            @JsonProperty("rowsTakenPayloads") Map<VersionedPartitionName, RowsTakenPayload> rowsTakenPayloads,
            @JsonProperty("pongPayload") PongPayload pongPayload) {
            this.rowsTakenPayloads = rowsTakenPayloads;
            this.pongPayload = pongPayload;
        }
    }

    public static class RowsTakenPayload implements Serializable {
        public final RingMember ringMember;
        public final long takeSessionId;
        public final String takeSharedKey;
        public final long txId;
        public final long leadershipToken;

        @JsonCreator
        public RowsTakenPayload(
            @JsonProperty("ringMember") RingMember ringMember,
            @JsonProperty("takeSessionId") long takeSessionId,
            @JsonProperty("takeSharedKey") String takeSharedKey,
            @JsonProperty("txId") long txId,
            @JsonProperty("leadershipToken") long leadershipToken) {
            this.ringMember = ringMember;
            this.takeSessionId = takeSessionId;
            this.takeSharedKey = takeSharedKey;
            this.txId = txId;
            this.leadershipToken = leadershipToken;
        }
    }

    public static class PongPayload implements Serializable {
        public final RingMember ringMember;
        public final long takeSessionId;
        public final String takeSharedKey;

        @JsonCreator
        public PongPayload(
            @JsonProperty("ringMember") RingMember ringMember,
            @JsonProperty("takeSessionId") long takeSessionId,
            @JsonProperty("takeSharedKey") String takeSharedKey) {
            this.ringMember = ringMember;
            this.takeSessionId = takeSessionId;
            this.takeSharedKey = takeSharedKey;
        }
    }

    @Override
    public boolean rowsTaken(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        long takeSessionId,
        String takeSharedKey,
        VersionedPartitionName versionedPartitionName,
        long txId,
        long localLeadershipToken) throws Exception {

        Ackable ackable = hostQueue.computeIfAbsent(remoteRingHost, ringHost -> new Ackable());
        ackable.semaphore.acquire();
        try {
            ackable.rowsTakenPayloads.get().put(versionedPartitionName, new RowsTakenPayload(localRingMember,
                takeSessionId,
                takeSharedKey,
                txId,
                localLeadershipToken));
        } finally {
            ackable.semaphore.release();
        }
        LOG.set(ValueType.COUNT, "flush>version>enqueue>" + name, flushVersion.incrementAndGet());
        synchronized (flushVersion) {
            flushVersion.notify();
        }
        return true;
    }

    @Override
    public boolean pong(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        long takeSessionId,
        String takeSharedKey) throws Exception {

        Ackable ackable = hostQueue.computeIfAbsent(remoteRingHost, ringHost -> new Ackable());
        ackable.semaphore.acquire();
        try {
            ackable.pongPayloads.set(new PongPayload(localRingMember, takeSessionId, takeSharedKey));
        } finally {
            ackable.semaphore.release();
        }
        LOG.set(ValueType.COUNT, "flush>version>enqueue>" + name, flushVersion.incrementAndGet());
        synchronized (flushVersion) {
            flushVersion.notify();
        }
        return true;
    }

    private void flushQueues(RingHost ringHost, Ackable ackable, long currentVersion) throws Exception {
        Map<VersionedPartitionName, RowsTakenPayload> rowsTaken;
        PongPayload pong;
        ackable.semaphore.acquire(Short.MAX_VALUE);
        try {
            rowsTaken = ackable.rowsTakenPayloads.getAndSet(Maps.newConcurrentMap());
            pong = ackable.pongPayloads.getAndSet(null);
        } finally {
            ackable.semaphore.release(Short.MAX_VALUE);
        }
        if (rowsTaken != null && !rowsTaken.isEmpty()) {
            LOG.inc("flush>rowsTaken>pow>" + UIO.chunkPower(rowsTaken.size(), 0));
        }

        if (rowsTaken != null && !rowsTaken.isEmpty() || pong != null) {
            flushExecutor.submit(() -> {
                try {
                    String endpoint = "/amza/ackBatch";
                    byte[] requestBytes = conf.asByteArray(new RowsTakenAndPong(rowsTaken, pong));
                    ringClient.call("",
                        new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(ringHost.getHost(), ringHost.getPort()) }),
                        "ackBatch",
                        httpClient -> {
                            HttpResponse response = httpClient.postBytes(endpoint, requestBytes, null);
                            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                                throw new NonSuccessStatusCodeException(response.getStatusCode(), response.getStatusReasonPhrase());
                            }
                            Boolean result = (Boolean) conf.asObject(response.getResponseBody());
                            return new ClientResponse<>(result, true);
                        });
                } catch (Exception x) {
                    LOG.warn("Failed to deliver acks for remote:{}", new Object[] { ringHost }, x);
                } finally {
                    ackable.running.set(false);
                    LOG.inc("flush>version>consume>" + name, flushVersion.incrementAndGet());
                    synchronized (flushVersion) {
                        if (currentVersion != flushVersion.get()) {
                            flushVersion.notify();
                        }
                    }
                }
            });
        } else {
            ackable.running.set(false);
            LOG.inc("flush>version>consume>" + name, flushVersion.incrementAndGet());
            synchronized (flushVersion) {
                if (currentVersion != flushVersion.get()) {
                    flushVersion.notify();
                }
            }
        }
    }

    //TODO include in flush?
    @Override
    public boolean invalidate(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        long takeSessionId,
        String takeSharedKey,
        VersionedPartitionName remoteVersionedPartitionName) {
        try {
            String endpoint = "/amza/invalidate/" + localRingMember.getMember() + "/" + takeSessionId + "/" + remoteVersionedPartitionName.toBase64();
            return ringClient.call("",
                new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(remoteRingHost.getHost(), remoteRingHost.getPort()) }),
                "invalidate",
                httpClient -> {
                    HttpResponse response = httpClient.postBytes(endpoint, takeSharedKey.getBytes(StandardCharsets.UTF_8), null);
                    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                        throw new NonSuccessStatusCodeException(response.getStatusCode(), response.getStatusReasonPhrase());
                    }
                    try {
                        return new ClientResponse<>(mapper.readValue(response.getResponseBody(), Boolean.class), true);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to deserialize response");
                    }
                });
        } catch (Exception x) {
            LOG.warn("Failed to invalidate for local:{} remote:{} session:{} partition:{}",
                new Object[] { localRingMember, remoteRingHost, takeSessionId, remoteVersionedPartitionName }, x);
            return false;
        } finally {
            amzaStats.invalidatesSent.increment();
        }
    }
}
