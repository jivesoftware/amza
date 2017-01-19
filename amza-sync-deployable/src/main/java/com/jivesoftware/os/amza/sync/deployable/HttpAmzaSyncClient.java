package com.jivesoftware.os.amza.sync.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import java.util.List;
import org.xerial.snappy.Snappy;

/**
 *
 */
public class HttpAmzaSyncClient implements AmzaSyncClient {

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String commitPath;
    private final String ensurePartitionPath;

    public HttpAmzaSyncClient(HttpClient httpClient, ObjectMapper mapper, String commitPath, String ensurePartitionPath) {
        this.httpClient = httpClient;
        this.mapper = mapper;
        this.commitPath = commitPath;
        this.ensurePartitionPath = ensurePartitionPath;
    }

    @Override
    public void commitRows(PartitionName toPartitionName, List<Row> rows) throws Exception {
        byte[] bytes = Snappy.compress(mapper.writeValueAsBytes(rows));
        String endpoint = commitPath + '/' + toPartitionName.toBase64();
        HttpResponse httpResponse = httpClient.postBytes(endpoint, bytes, null);
        if (!isSuccessStatusCode(httpResponse.getStatusCode())) {
            throw new SyncClientException("Empty response from sync receiver");
        }
    }

    @Override
    public void ensurePartition(PartitionName toPartitionName, PartitionProperties properties, int ringSize) throws Exception {
        String endpoint = ensurePartitionPath + '/' + toPartitionName.toBase64() + '/' + ringSize;
        String json = mapper.writeValueAsString(properties);
        HttpResponse httpResponse = httpClient.postJson(endpoint, json, null);
        if (!isSuccessStatusCode(httpResponse.getStatusCode())) {
            throw new SyncClientException("Empty response from sync receiver");
        }
    }

    private static boolean isSuccessStatusCode(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
