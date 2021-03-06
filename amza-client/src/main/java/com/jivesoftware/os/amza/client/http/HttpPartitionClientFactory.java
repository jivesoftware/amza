package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;

/**
 * @author jonathan.colt
 */
public class HttpPartitionClientFactory implements PartitionClientFactory<HttpClient, HttpClientException> {

    @Override
    public PartitionClient create(PartitionName partitionName,
        AmzaClientCallRouter<HttpClient, HttpClientException> partitionCallRouter,
        long awaitLeaderElectionForNMillis,
        long debugClientCount,
        long debugClientCountInterval) throws Exception {

        HttpRemotePartitionCaller remotePartitionCaller = new HttpRemotePartitionCaller(partitionCallRouter, partitionName);
        return new AmzaPartitionClient<>(partitionName, partitionCallRouter, remotePartitionCaller, awaitLeaderElectionForNMillis,
            debugClientCount, debugClientCountInterval);
    }

}
