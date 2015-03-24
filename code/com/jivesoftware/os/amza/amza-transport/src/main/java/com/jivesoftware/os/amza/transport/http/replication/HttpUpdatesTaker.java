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
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClient;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfig;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfiguration;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactory;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactoryProvider;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpRequestHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class HttpUpdatesTaker implements UpdatesTaker {

    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();

    @Override
    public void takeUpdates(RingHost ringHost,
            RegionName partitionName,
            long transactionId,
            WALScan tookRowUpdates) throws Exception {

        RowUpdates changeSet = new RowUpdates(transactionId, partitionName, new ArrayList<byte[]>());
        RowUpdates took = getRequestHelper(ringHost).executeRequest(changeSet, "/amza/changes/take", RowUpdates.class, null);
        if (took == null) {
            return;
        }
        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        for (byte[] row : took.getChanges()) {
            RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
            tookRowUpdates.row(walr.getTransactionId(), walr.getKey(), walr.getValue());
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
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        HttpRequestHelper requestHelper = new HttpRequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }
}
