package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import java.util.List;

/**
 *
 */
public class HttpAmzaSyncClient implements AmzaSyncClient {

    private final HttpRequestHelper httpRequestHelper;
    private final String commitPath;
    private final String ensurePartitionPath;

    public HttpAmzaSyncClient(HttpRequestHelper httpRequestHelper, String commitPath, String ensurePartitionPath) {
        this.httpRequestHelper = httpRequestHelper;
        this.commitPath = commitPath;
        this.ensurePartitionPath = ensurePartitionPath;
    }

    @Override
    public void commitRows(PartitionName toPartitionName, List<Row> rows) throws Exception {
        String endpoint = commitPath + '/' + toPartitionName.toBase64();
        String result = httpRequestHelper.executeRequest(rows, endpoint, String.class, null);
        if (result == null) {
            throw new SyncClientException("Empty response from sync receiver");
        }
    }

    @Override
    public void ensurePartition(PartitionName toPartitionName, PartitionProperties properties, int ringSize) throws Exception {
        String endpoint = ensurePartitionPath + '/' + toPartitionName.toBase64() + '/' + ringSize;
        String result = httpRequestHelper.executeRequest(properties, endpoint, String.class, null);
        if (result == null) {
            throw new SyncClientException("Empty response from sync receiver");
        }
    }
}
