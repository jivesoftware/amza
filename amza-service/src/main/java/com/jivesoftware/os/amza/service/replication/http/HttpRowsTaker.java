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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.BAInterner;
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
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import org.xerial.snappy.SnappyInputStream;

public class HttpRowsTaker implements RowsTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final TenantAwareHttpClient<String> ringClient;
    private final ObjectMapper mapper;
    private final StreamingTakesConsumer streamingTakesConsumer;

    public HttpRowsTaker(AmzaStats amzaStats,
        TenantAwareHttpClient<String> ringClient,
        ObjectMapper mapper,
        BAInterner interner) {
        this.amzaStats = amzaStats;
        this.ringClient = ringClient;
        this.mapper = mapper;
        this.streamingTakesConsumer = new StreamingTakesConsumer(interner);
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

    @Override
    public boolean rowsTaken(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        long takeSessionId,
        String takeSharedKey,
        VersionedPartitionName versionedPartitionName,
        long txId,
        long localLeadershipToken) {
        try {
            String endpoint = "/amza/rows/taken/" + localRingMember.getMember()
                + "/" + takeSessionId
                + "/" + versionedPartitionName.toBase64()
                + "/" + txId
                + "/" + localLeadershipToken;
            String sharedKeyJson = mapper.writeValueAsString(takeSharedKey);
            return ringClient.call("",
                new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(remoteRingHost.getHost(), remoteRingHost.getPort()) }),
                "rowsTaken",
                httpClient -> {
                    HttpResponse response = httpClient.postJson(endpoint, sharedKeyJson, null);
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
            LOG.warn("Failed to deliver acks for local:{} remote:{} partition:{} tx:{}",
                new Object[] { localRingMember, remoteRingHost, versionedPartitionName, txId }, x);
            return false;
        }
    }

    @Override
    public boolean pong(RingMember localRingMember, RingMember remoteRingMember, RingHost remoteRingHost, long takeSessionId, String takeSharedKey) {
        try {
            String endpoint = "/amza/pong/" + localRingMember.getMember() + "/" + takeSessionId;
            String sharedKeyJson = mapper.writeValueAsString(takeSharedKey);
            return ringClient.call("",
                new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(remoteRingHost.getHost(), remoteRingHost.getPort()) }),
                "rowsTaken",
                httpClient -> {
                    HttpResponse response = httpClient.postJson(endpoint, sharedKeyJson, null);
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
            LOG.warn("Failed to deliver pong for local:{} remote:{} session:{}",
                new Object[] { localRingMember, remoteRingHost, takeSessionId }, x);
            return false;
        } finally {
            amzaStats.pongsSent.increment();
        }
    }

    @Override
    public boolean invalidate(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        long takeSessionId,
        String takeSharedKey,
        VersionedPartitionName remoteVersionedPartitionName) {
        try {
            String endpoint = "/amza/invalidate/" + localRingMember.getMember() + "/" + takeSessionId + "/" + remoteVersionedPartitionName.toBase64();
            String sharedKeyJson = mapper.writeValueAsString(takeSharedKey);
            return ringClient.call("",
                new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(remoteRingHost.getHost(), remoteRingHost.getPort()) }),
                "invalidate",
                httpClient -> {
                    HttpResponse response = httpClient.postJson(endpoint, sharedKeyJson, null);
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
