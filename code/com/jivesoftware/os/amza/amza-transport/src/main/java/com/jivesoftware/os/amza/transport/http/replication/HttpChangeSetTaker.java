package com.jivesoftware.os.amza.transport.http.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.storage.TransactionEntry;
import com.jivesoftware.os.amza.storage.json.StringRowMarshaller;
import com.jivesoftware.os.amza.transport.http.replication.endpoints.StringArrayRowReader;
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
    public <K, V> boolean take(RingHost ringHost,
            TableName<K, V> partitionName,
            long transationId,
            TransactionSetStream transactionSetStream) throws Exception {
        ChangeSet changeSet = new ChangeSet(transationId, partitionName, new ArrayList<String>());
        ChangeSet took = getRequestHelper(ringHost).executeRequest(changeSet, "/amza/changes/take", ChangeSet.class, null);
        if (took == null) {
            return true;
        }
        TableRowReader<String> rowReader = new StringArrayRowReader(took.getChanges());
        final StringRowMarshaller jsonPartitionRowMarshaller = new StringRowMarshaller(new ObjectMapper(), changeSet.getTableName());
        final ConcurrentNavigableMap<K, TimestampedValue<V>> changes = new ConcurrentSkipListMap<>();
        rowReader.read(false, new TableRowReader.Stream<String>() {
            @Override
            public boolean stream(String keyValueTimestamp) throws Exception {
                TransactionEntry<K, V> te = jsonPartitionRowMarshaller.fromRow(keyValueTimestamp);
                changes.put(te.getKey(), te.getValue());
                return true;
            }
        });
        TransactionSet<K, V> transactionSet = new TransactionSet<>(took.getHighestTransactionId(), changes);
        return transactionSetStream.stream(transactionSet);
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