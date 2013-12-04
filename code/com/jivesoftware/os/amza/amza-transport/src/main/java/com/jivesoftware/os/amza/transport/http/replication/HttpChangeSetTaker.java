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
import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.storage.TransactionEntry;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class HttpChangeSetTaker implements ChangeSetTaker {

    private final ConcurrentHashMap<RingHost, RequestHelper> requestHelpers = new ConcurrentHashMap<>();

    @Override
    public void take(RingHost ringHost,
            TableName partitionName,
            long transationId,
            TransactionSetStream transactionSetStream) throws Exception {
        ChangeSet changeSet = new ChangeSet(transationId, partitionName, new ArrayList<byte[]>());
        ChangeSet took = getRequestHelper(ringHost).executeRequest(changeSet, "/amza/changes/take", ChangeSet.class, null);
        if (took == null) {
            return;
        }
        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        final ConcurrentNavigableMap<TableIndexKey, BinaryTimestampedValue> changes = new ConcurrentSkipListMap<>();
        for (byte[] row : took.getChanges()) {
            TransactionEntry te = rowMarshaller.fromRow(row);
            changes.put(te.getKey(), te.getValue());
        }
        TransactionSet transactionSet = new TransactionSet(took.getHighestTransactionId(), changes);
        transactionSetStream.stream(transactionSet);
    }

    RequestHelper getRequestHelper(RingHost ringHost) {
        RequestHelper requestHelper = requestHelpers.get(ringHost);
        if (requestHelper == null) {
            requestHelper = buildRequestHelper(ringHost.getHost(), ringHost.getPort());
            RequestHelper had = requestHelpers.putIfAbsent(ringHost, requestHelper);
            if (had != null) {
                requestHelper = had;
            }
        }
        return requestHelper;
    }

    RequestHelper buildRequestHelper(String host, int port) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        RequestHelper requestHelper = new RequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }
}
