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
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClient;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfig;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfiguration;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactory;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactoryProvider;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpRequestHelper;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.mutable.MutableLong;

public class HttpUpdatesSender implements UpdatesSender {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaStats amzaStats;
    private final ConcurrentHashMap<RingHost, HttpRequestHelper> requestHelpers = new ConcurrentHashMap<>();

    public HttpUpdatesSender(AmzaStats amzaStats) {
        this.amzaStats = amzaStats;
    }

    @Override
    public void sendUpdates(final RingHost ringHost, final RegionName regionName, WALScanable changes) throws Exception {

        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        final List<Long> rowTxIds = new ArrayList<>();
        final List<byte[]> rows = new ArrayList<>();
        final MutableLong smallestTx = new MutableLong(Long.MAX_VALUE);
        final MutableLong wrote = new MutableLong();

        changes.rowScan(new WALScan() {
            @Override
            public boolean row(long txId, WALKey key, WALValue value) throws Exception {
                if (txId < smallestTx.longValue()) {
                    smallestTx.setValue(txId);
                }
                rowTxIds.add(txId);
                byte[] rowBytes = rowMarshaller.toRow(key, value);
                wrote.add(rowBytes.length);
                rows.add(rowBytes);
                return true;
            }
        });
        if (!rows.isEmpty()) {
            LOG.debug("Pushing " + rows.size() + " changes to " + ringHost);
            RowUpdates changeSet = new RowUpdates(-1, regionName, rowTxIds, rows);
            getRequestHelper(ringHost).executeRequest(changeSet, "/amza/changes/add", Boolean.class, false);
            amzaStats.replicated(ringHost, regionName, rows.size(), smallestTx.longValue());
            amzaStats.netStats.wrote.addAndGet(wrote.longValue());
        } else {
            amzaStats.replicated(ringHost, regionName, 0, Long.MAX_VALUE);
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
