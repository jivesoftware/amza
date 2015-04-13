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
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClient;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfig;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfiguration;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactory;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactoryProvider;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpRequestHelper;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpStreamResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.mutable.MutableLong;

public class HttpUpdatesTaker implements UpdatesTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaStats amzaStats;
    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();

    public HttpUpdatesTaker(AmzaStats amzaStats) {
        this.amzaStats = amzaStats;
    }

    @Override
    public Map<RingHost, Long> streamingTakeUpdates(RingHost ringHost,
        RegionName regionName,
        long transactionId,
        RowStream tookRowUpdates) throws Exception {

        TakeRequest takeRequest = new TakeRequest(transactionId, regionName);

        long t1 = System.currentTimeMillis();
        HttpStreamResponse httpStreamResponse = getRequestHelper(ringHost).executeStreamingPostRequest(takeRequest, "/amza/changes/streamingTake");
        long t2 = System.currentTimeMillis();
        try {
            BufferedInputStream bis = new BufferedInputStream(httpStreamResponse.getInputStream(), 8096); // TODO config??
            long t3 = System.currentTimeMillis();
            long t4 = -1;
            long updates = 0;
            final MutableLong read = new MutableLong();
            Map<RingHost, Long> neighborsHighwaterMarks = new HashMap<>();
            try (DataInputStream dis = new DataInputStream(bis)) {
                byte eosMarks;
                while ((eosMarks = dis.readByte()) == 1) {
                    if (t4 < 0) {
                        t4 = System.currentTimeMillis();
                    }
                    byte[] ringHostBytes = new byte[dis.readInt()];
                    dis.readFully(ringHostBytes);
                    long highwaterMark = dis.readLong();
                    neighborsHighwaterMarks.put(RingHost.fromBytes(ringHostBytes), highwaterMark);
                    read.add(1 + 4 + ringHostBytes.length + 8);
                }
                while ((eosMarks = dis.readByte()) == 1) {
                    long rowTxId = dis.readLong();
                    long rowType = dis.readByte();
                    byte[] rowBytes = new byte[dis.readInt()];
                    dis.readFully(rowBytes);
                    read.add(1 + 8 + 1 + 4 + rowBytes.length);
                    tookRowUpdates.row(rowType, rowTxId, eosMarks, rowBytes);
                    updates++;
                }
            }
            amzaStats.netStats.read.addAndGet(read.longValue());
            long t5 = System.currentTimeMillis();
            if (!regionName.isSystemRegion()) {
                LOG.debug("Take {}: Execute={}ms GetInputStream={}ms FirstByte={}ms ReadFully={}ms TotalTime={}ms TotalUpdates={} TotalBytes={}",
                    regionName.getRegionName(), (t2 - t1), (t3 - t2), (t4 - t3), (t5 - t4), (t5 - t1), updates, read.longValue());
            }
            return neighborsHighwaterMarks;
        } finally {
            httpStreamResponse.close();
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
