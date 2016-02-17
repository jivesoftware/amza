package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;

/**
 *
 * @author jonathan.colt
 */
public class HttpPartitionClientFactory implements PartitionClientFactory<HttpClient, HttpClientException> {

    private final BAInterner interner;

    public HttpPartitionClientFactory(BAInterner interner) {
        this.interner = interner;
    }

    @Override
    public PartitionClient create(PartitionName partitionName,
        AmzaClientCallRouter<HttpClient, HttpClientException> partitionCallRouter,
        long awaitLeaderElectionForNMillis) throws Exception {

        HttpRemotePartitionCaller remotePartitionCaller = new HttpRemotePartitionCaller(partitionCallRouter, partitionName);
        return new AmzaPartitionClient(interner, partitionName, partitionCallRouter, remotePartitionCaller, awaitLeaderElectionForNMillis);
    }

}
