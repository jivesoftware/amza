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
package com.jivesoftware.os.amza.transport.http.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.shared.take.RowsTaker;
import com.jivesoftware.os.amza.shared.take.StreamingTakesConsumer;
import com.jivesoftware.os.amza.shared.take.StreamingTakesConsumer.StreamingTakeConsumed;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfig;
import com.jivesoftware.os.routing.bird.http.client.HttpClientConfiguration;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import java.io.BufferedInputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class HttpRowsTaker implements RowsTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();
    private final StreamingTakesConsumer streamingTakesConsumer = new StreamingTakesConsumer();

    public HttpRowsTaker(AmzaStats amzaStats) {
        this.amzaStats = amzaStats;
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
        RowStream rowStream) {

        HttpStreamResponse httpStreamResponse;
        try {
            httpStreamResponse = getRequestHelper(remoteRingHost).executeStreamingPostRequest(null,
                "/amza/rows/stream/" + localRingMember.getMember() + "/" + remoteVersionedPartitionName.toBase64() + "/" + remoteTxId);
        } catch (Exception e) {
            return new StreamingRowsResult(e, null, null);
        }
        try {
            BufferedInputStream bis = new BufferedInputStream(httpStreamResponse.getInputStream(), 8096); // TODO config??
            StreamingTakeConsumed consumed = streamingTakesConsumer.consume(bis, rowStream);
            amzaStats.netStats.read.addAndGet(consumed.bytes);
            return new StreamingRowsResult(null, null, consumed.isOnline ? consumed.neighborsHighwaterMarks : null);
        } catch (Exception e) {
            return new StreamingRowsResult(null, e, null);
        } finally {
            httpStreamResponse.close();
        }
    }

    @Override
    public boolean rowsTaken(RingMember localRingMember,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        VersionedPartitionName versionedPartitionName,
        long txId) {
        try {
            return getRequestHelper(remoteRingHost).executeRequest(null,
                "/amza/rows/taken/"
                    + localRingMember.getMember() + "/"
                    + versionedPartitionName.toBase64() + "/"
                    + txId,
                Boolean.class, false);
        } catch (Exception x) {
            LOG.warn("Failed to deliver acks for local:{} remote:{} partition:{} tx:{}",
                new Object[] { localRingMember, remoteRingHost, versionedPartitionName, txId }, x);
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
