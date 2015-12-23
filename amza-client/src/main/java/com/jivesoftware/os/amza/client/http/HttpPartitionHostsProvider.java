package com.jivesoftware.os.amza.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.io.IOException;
import java.util.Optional;

/**
 * @author jonathan.colt
 */
public class HttpPartitionHostsProvider implements PartitionHostsProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final TenantAwareHttpClient<String> tenantAwareHttpClient;
    private final ObjectMapper mapper;

    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public HttpPartitionHostsProvider(TenantAwareHttpClient<String> tenantAwareHttpClient, ObjectMapper mapper) {
        this.tenantAwareHttpClient = tenantAwareHttpClient;
        this.mapper = mapper;
    }

    @Override
    public void ensurePartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
        String base64PartitionName = partitionName.toBase64();
        String partitionPropertiesString = mapper.writeValueAsString(partitionProperties);
        byte[] intBuffer = new byte[4];
        Ring partitionsRing = tenantAwareHttpClient.call("", roundRobinStrategy, "configPartition", (client) -> {

            HttpStreamResponse got = client.streamingPost("/amza/v1/configPartition/" + base64PartitionName + "/" + desiredRingSize,
                partitionPropertiesString, null);
            try {
                if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                    try {
                        FilerInputStream fis = new FilerInputStream(got.getInputStream());
                        int ringSize = UIO.readInt(fis, "ringSize", intBuffer);
                        int leaderIndex = -1;
                        RingMemberAndHost[] ring = new RingMemberAndHost[ringSize];
                        for (int i = 0; i < ringSize; i++) {
                            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(fis, "ringMember", intBuffer));
                            RingHost ringHost = RingHost.fromBytes(UIO.readByteArray(fis, "ringHost", intBuffer));
                            ring[i] = new RingMemberAndHost(ringMember, ringHost);
                            if (UIO.readBoolean(fis, "leader")) {
                                if (leaderIndex == -1) {
                                    leaderIndex = i;
                                } else {
                                    throw new RuntimeException("We suck! Gave back more than one leader!");
                                }
                            }
                        }
                        return new ClientCall.ClientResponse<>(new Ring(leaderIndex, ring), true);
                    } catch (Exception x) {
                        throw new RuntimeException("Failed loading routes for " + partitionName, x);
                    }
                }
            } finally {
                got.close();
            }
            throw new RuntimeException("Failed to config partition:" + partitionName);
        });

        HostPort[] orderHostPorts = new HostPort[partitionsRing.members.length];
        for (int i = 0; i < orderHostPorts.length; i++) {
            RingHost ringHost = partitionsRing.members[i].ringHost;
            orderHostPorts[i] = new HostPort(ringHost.getHost(), ringHost.getPort());
        }
        NextClientStrategy strategy = new ConnectionDescriptorSelectiveStrategy(orderHostPorts);
        tenantAwareHttpClient.call("", strategy, "ensurePartition", (client) -> {

            HttpResponse got = client.postJson("/amza/v1/ensurePartition/" + base64PartitionName + "/" + 30_000, // TODO config
                partitionPropertiesString, null);

            if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                return new ClientCall.ClientResponse<>(null, true);
            }
            throw new RuntimeException("Failed to ensure partition:" + partitionName);
        });
    }

    @Override
    public Ring getPartitionHosts(PartitionName partitionName, Optional<RingMemberAndHost> useHost, long waitForLeaderElection) throws HttpClientException {

        NextClientStrategy strategy = useHost.map(
            (RingMemberAndHost value) -> (NextClientStrategy) new ConnectionDescriptorSelectiveStrategy(new HostPort[]{
            new HostPort(value.ringHost.getHost(), value.ringHost.getPort())
        })).orElse(roundRobinStrategy);
        byte[] intBuffer = new byte[4];
        return tenantAwareHttpClient.call("", strategy, "getPartitionHosts", (client) -> {

            HttpStreamResponse got = client.streamingPost("/amza/v1/ring/"
                + partitionName.toBase64() + "/" + waitForLeaderElection, "", null);
            try {
                if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                    FilerInputStream fis = null;
                    try {
                        fis = new FilerInputStream(got.getInputStream());
                        int ringSize = UIO.readInt(fis, "ringSize", intBuffer);
                        int leaderIndex = -1;
                        RingMemberAndHost[] ring = new RingMemberAndHost[ringSize];
                        for (int i = 0; i < ringSize; i++) {
                            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(fis, "ringMember", intBuffer));
                            RingHost ringHost = RingHost.fromBytes(UIO.readByteArray(fis, "ringHost", intBuffer));
                            ring[i] = new RingMemberAndHost(ringMember, ringHost);
                            if (UIO.readBoolean(fis, "leader")) {
                                if (leaderIndex == -1) {
                                    leaderIndex = i;
                                } else {
                                    throw new RuntimeException("We suck! Gave back more than one leader!");
                                }
                            }
                        }
                        return new ClientCall.ClientResponse<>(new Ring(leaderIndex, ring), true);

                    } catch (Exception x) {
                        throw new RuntimeException("Failed loading routes for " + partitionName, x);
                    } finally {
                        if (fis != null) {
                            try {
                                fis.close();
                            } catch (IOException e) {
                                LOG.warn("Failed to close input stream", e);
                            }
                        }
                    }
                }
            } finally {
                got.close();
            }
            throw new RuntimeException("No routes to partition:" + partitionName);
        });
    }


}
