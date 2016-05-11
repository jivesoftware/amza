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
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.StreamingTakesConsumer;
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
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.xerial.snappy.SnappyInputStream;

public class HttpAvailableRowsTaker implements AvailableRowsTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();
    private final StreamingTakesConsumer streamingTakesConsumer;

    public HttpAvailableRowsTaker(BAInterner interner, String name, long interruptBlockingReadsIfLingersForNMillis) {
        this.streamingTakesConsumer = new StreamingTakesConsumer(interner, name, interruptBlockingReadsIfLingersForNMillis);
    }

    @Override
    public void availableRowsStream(RingMember localRingMember,
        TimestampedRingHost localTimestampedRingHost,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        boolean system,
        long takeSessionId,
        long timeoutMillis,
        AvailableStream availableStream) throws Exception {

        HttpStreamResponse httpStreamResponse = getRequestHelper(remoteRingHost).executeStreamingPostRequest(null,
            "/amza/rows/available"
            + "/" + localRingMember.getMember()
            + "/" + localTimestampedRingHost.ringHost.toCanonicalString()
            + "/" + system
            + "/" + localTimestampedRingHost.timestampId
            + "/" + takeSessionId
            + "/" + timeoutMillis);
        try {
            BufferedInputStream bis = new BufferedInputStream(httpStreamResponse.getInputStream(), 8096); // TODO config??
            DataInputStream dis = new DataInputStream(new SnappyInputStream(bis));
            streamingTakesConsumer.consume(dis, availableStream);
        } finally {
            httpStreamResponse.close();
        }
    }

    HttpRequestHelper getRequestHelper(RingHost ringHost) {
        return requestHelpers.computeIfAbsent(ringHost, (t) -> buildRequestHelper(ringHost.getHost(), ringHost.getPort()));
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
