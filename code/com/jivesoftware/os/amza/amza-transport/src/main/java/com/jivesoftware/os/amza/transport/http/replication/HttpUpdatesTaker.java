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
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.mutable.MutableLong;

public class HttpUpdatesTaker implements UpdatesTaker {

    private final AmzaStats amzaStats;
    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();

    public HttpUpdatesTaker(AmzaStats amzaStats) {
        this.amzaStats = amzaStats;
    }

    @Override
    public void streamingTakeUpdates(RingHost ringHost,
        RegionName regionName,
        long transactionId,
        RowStream tookRowUpdates) throws Exception {

        RowUpdates changeSet = new RowUpdates(transactionId, regionName, new ArrayList<Long>(), new ArrayList<byte[]>());

        InputStream inputStream = getRequestHelper(ringHost).executeStreamingPostRequest(changeSet, "/amza/changes/streamingTake");
        BufferedInputStream bis = new BufferedInputStream(inputStream, 8096); // TODO config??
        final MutableLong read = new MutableLong();
        try (DataInputStream dis = new DataInputStream(bis)) {
            byte eosMarks;
            while ((eosMarks = dis.readByte()) == 1) {

                long rowTxId = dis.readLong();
                long rowType = dis.readByte();
                byte[] rowBytes = new byte[dis.readInt()];
                dis.readFully(rowBytes);
                read.add(rowBytes.length);
                tookRowUpdates.row(rowType, rowTxId, eosMarks, rowBytes);
            }
        }
        amzaStats.netStats.read.addAndGet(read.longValue());
    }

    HttpRequestHelper getRequestHelper(RingHost ringHost
    ) {
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

    HttpRequestHelper buildRequestHelper(String host, int port
    ) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        HttpRequestHelper requestHelper = new HttpRequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }
}
