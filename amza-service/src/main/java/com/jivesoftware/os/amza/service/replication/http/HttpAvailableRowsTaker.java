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
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker;
import com.jivesoftware.os.amza.service.take.StreamingTakesConsumer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.NonSuccessStatusCodeException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import org.xerial.snappy.SnappyInputStream;

public class HttpAvailableRowsTaker implements AvailableRowsTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TenantAwareHttpClient<String> ringClient;
    private final StreamingTakesConsumer streamingTakesConsumer;
    private final ObjectMapper mapper;

    public HttpAvailableRowsTaker(TenantAwareHttpClient<String> ringClient,
        AmzaInterner amzaInterner,
        ObjectMapper mapper) {
        this.ringClient = ringClient;
        this.streamingTakesConsumer = new StreamingTakesConsumer(amzaInterner);
        this.mapper = mapper;
    }

    @Override
    public void availableRowsStream(RingMember localRingMember,
        TimestampedRingHost localTimestampedRingHost,
        RingMember remoteRingMember,
        RingHost remoteRingHost,
        boolean system,
        long takeSessionId,
        String takeSharedKey,
        long timeoutMillis,
        AvailableStream availableStream,
        PingStream pingStream) throws Exception {

        String endpoint = "/amza/rows/available"
            + "/" + localRingMember.getMember()
            + "/" + localTimestampedRingHost.ringHost.toCanonicalString()
            + "/" + system
            + "/" + localTimestampedRingHost.timestampId
            + "/" + takeSessionId
            + "/" + timeoutMillis;
        String sharedKeyJson = mapper.writeValueAsString(takeSharedKey);
        HttpStreamResponse httpStreamResponse = ringClient.call("",
            new ConnectionDescriptorSelectiveStrategy(new HostPort[] { new HostPort(remoteRingHost.getHost(), remoteRingHost.getPort()) }),
            "availableRowsStream",
            httpClient -> {
                HttpStreamResponse response = httpClient.streamingPost(endpoint, sharedKeyJson, null);
                if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                    throw new NonSuccessStatusCodeException(response.getStatusCode(), response.getStatusReasonPhrase());
                }
                return new ClientResponse<>(response, true);
            });
        try {
            BufferedInputStream bis = new BufferedInputStream(httpStreamResponse.getInputStream(), 8192); // TODO config??
            DataInputStream dis = new DataInputStream(new SnappyInputStream(bis));
            streamingTakesConsumer.consume(dis, availableStream, pingStream);
        } finally {
            httpStreamResponse.close();
        }
    }

}
