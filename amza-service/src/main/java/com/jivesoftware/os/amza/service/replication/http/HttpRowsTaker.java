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
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfig;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfiguration;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.xerial.snappy.SnappyInputStream;

public class HttpRowsTaker implements RowsTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();
    private final StreamingTakesConsumer streamingTakesConsumer;

    public HttpRowsTaker(AmzaStats amzaStats, BAInterner interner) {
        this.amzaStats = amzaStats;
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
        long remoteTxId,
        long localLeadershipToken,
        RowStream rowStream) {

        HttpStreamResponse httpStreamResponse;
        try {
            httpStreamResponse = getRequestHelper(remoteRingHost).executeStreamingPostRequest(null,
                "/amza/rows/stream/" + localRingMember.getMember()
                + "/" + remoteVersionedPartitionName.toBase64()
                + "/" + remoteTxId
                + "/" + localLeadershipToken);
        } catch (IOException | HttpClientException e) {
            return new StreamingRowsResult(e, null, -1, -1, null);
        }
        try {
            BufferedInputStream bis = new BufferedInputStream(httpStreamResponse.getInputStream(), 8096); // TODO config??
            DataInputStream dis = new DataInputStream(new SnappyInputStream(bis));
            StreamingTakeConsumed consumed = streamingTakesConsumer.consume(dis, rowStream);
            amzaStats.netStats.read.addAndGet(consumed.bytes);
            Map<RingMember, Long> otherHighwaterMarks = consumed.isOnline ? consumed.neighborsHighwaterMarks : null;
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
        VersionedPartitionName versionedPartitionName,
        long txId,
        long localLeadershipToken) {
        try {
            return getRequestHelper(remoteRingHost).executeRequest(null,
                "/amza/rows/taken/" + localRingMember.getMember()
                + "/" + takeSessionId
                + "/" + versionedPartitionName.toBase64()
                + "/" + txId
                + "/" + localLeadershipToken,
                Boolean.class, false);
        } catch (Exception x) {
            LOG.warn("Failed to deliver acks for local:{} remote:{} partition:{} tx:{}",
                new Object[]{localRingMember, remoteRingHost, versionedPartitionName, txId}, x);
            return false;
        }
    }

    HttpRequestHelper getRequestHelper(RingHost ringHost) {
        HttpRequestHelper requestHelper = requestHelpers.get(ringHost);
        if (requestHelper == null) {
            requestHelper = buildRequestHelper(ringHost.getHost(), ringHost.getPort());
            HttpRequestHelper had = requestHelpers.putIfAbsent(ringHost, requestHelper);
            if (had != null) {
                requestHelper = had;
            }
        }
        return requestHelper;
    }

    HttpRequestHelper buildRequestHelper(String host, int port) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        HttpRequestHelper requestHelper = new HttpRequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }

}
