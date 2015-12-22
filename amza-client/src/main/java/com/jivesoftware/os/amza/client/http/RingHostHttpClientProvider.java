package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;

/**
 *
 * @author jonathan.colt
 */
public class RingHostHttpClientProvider implements RingHostClientProvider<HttpClient, HttpClientException> {

    private final TenantAwareHttpClient<String> tenantAwareHttpClient;

    public RingHostHttpClientProvider(TenantAwareHttpClient<String> tenantAwareHttpClient) {
        this.tenantAwareHttpClient = tenantAwareHttpClient;
    }

    @Override
    public <R> R call(PartitionName partitionName,
        RingMember leader,
        RingMemberAndHost ringMemberAndHost,
        String family,
        PartitionCall<HttpClient, R, HttpClientException> clientCall) throws HttpClientException {

        ConnectionDescriptorSelectiveStrategy strategy = new ConnectionDescriptorSelectiveStrategy(new HostPort[]{
            new HostPort(ringMemberAndHost.ringHost.getHost(), ringMemberAndHost.ringHost.getPort())
        });

        return tenantAwareHttpClient.call("", strategy, family, (client) -> {
            PartitionResponse<R> response = clientCall.call(leader, ringMemberAndHost.ringMember, client);
            return new ClientResponse<R>(response.response, response.responseComplete);
        });
    }
}
