package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;

/**
 *
 * @author jonathan.colt
 */
public interface HttpClientProvider {

    <R> R call(PartitionName partitionName,
        RingHost ringHost,
        String family,
        PartitionCall<HttpClient, R, HttpClientException> clientCall) throws HttpClientException;

}
