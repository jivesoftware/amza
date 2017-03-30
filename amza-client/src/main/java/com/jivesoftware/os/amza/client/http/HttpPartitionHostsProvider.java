package com.jivesoftware.os.amza.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.RingPartitionProperties;
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
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpStatus;

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

    private static class RingMembersChangedException extends RuntimeException {

        public RingMembersChangedException(String message) {
            super(message);
        }
    }

    @Override
    public RingPartitionProperties getRingPartitionProperties(PartitionName partitionName) throws Exception {
        String base64PartitionName = partitionName.toBase64();
        return tenantAwareHttpClient.call("", roundRobinStrategy, "configPartition", (client) -> {
            HttpResponse got = client.get("/amza/v1/properties/" + base64PartitionName, null);
            if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                try {
                    return new ClientResponse<>(mapper.readValue(got.getResponseBody(), RingPartitionProperties.class), true);
                } catch (IOException x) {
                    throw new RuntimeException("Failed getting properties for " + partitionName, x);
                }
            }
            throw new RuntimeException("Failed to get properties:" + partitionName + " statusCode:" + got.getStatusCode());
        });
    }

    @Override
    public void ensurePartition(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
        long timeoutAfterTimestamp = System.currentTimeMillis() + 30_000; //TODO config
        while (System.currentTimeMillis() < timeoutAfterTimestamp) {
            try {
                ensure(partitionName, desiredRingSize, partitionProperties);
                return;
            } catch (RingMembersChangedException e) {
                LOG.warn("Ring membership for {} has changed, retrying...", partitionName);
                Thread.sleep(10); //TODO config
            }
        }
        throw new TimeoutException("Failed to ensure partition: " + partitionName);
    }

    private void ensure(PartitionName partitionName, int desiredRingSize, PartitionProperties partitionProperties) throws Exception {
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
                            byte[] ringMemberBytes = UIO.readByteArray(fis, "ringMember", intBuffer);
                            RingMember ringMember = new RingMember(ringMemberBytes);
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
            throw new RuntimeException("Failed to config partition:" + partitionName + " statusCode:" + got.getStatusCode());
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
                return new ClientResponse<>(null, true);
            } else if (got.getStatusCode() == HttpStatus.SC_CONFLICT) {
                throw new RingMembersChangedException("Ring members have changed");
            } else {
                throw new RuntimeException("Failed to ensure partition: " + partitionName + " statusCode: " + got.getStatusCode());
            }
        });
    }

    @Override
    public Ring getPartitionHosts(PartitionName partitionName, Optional<RingMemberAndHost> useHost, long waitForLeaderElection) throws HttpClientException {

        NextClientStrategy strategy = useHost.map((ringMemberAndHost) -> {
            HostPort[] hostPorts = { new HostPort(ringMemberAndHost.ringHost.getHost(), ringMemberAndHost.ringHost.getPort()) };
            return (NextClientStrategy) new ConnectionDescriptorSelectiveStrategy(hostPorts);
        }).orElse(roundRobinStrategy);
        byte[] intBuffer = new byte[4];
        Ring leaderlessRing = tenantAwareHttpClient.call("", strategy, "getPartitionHosts", (client) -> {

            HttpStreamResponse got = client.streamingPost("/amza/v1/ring/"
                + partitionName.toBase64(), "", null);
            Ring ring = consumeRing(partitionName, got, intBuffer);
            return new ClientCall.ClientResponse<>(ring, true);
        });
        if (waitForLeaderElection > 0) {

            RingMemberAndHost[] actualRing = leaderlessRing.actualRing();
            HostPort[] chooseFrom = new HostPort[actualRing.length];
            for (int i = 0; i < chooseFrom.length; i++) {
                RingHost ringHost = actualRing[i].ringHost;
                chooseFrom[i] = new HostPort(ringHost.getHost(), ringHost.getPort());
            }

            strategy = new ConnectionDescriptorSelectiveStrategy(chooseFrom);
            return tenantAwareHttpClient.call("", strategy, "ringLeader", (client) -> {

                HttpStreamResponse got = client.streamingPost("/amza/v1/ringLeader/"
                    + partitionName.toBase64() + "/" + waitForLeaderElection, "", null);
                Ring ring = consumeRing(partitionName, got, intBuffer);
                return new ClientCall.ClientResponse<>(ring, true);
            });

        } else {
            return leaderlessRing;
        }
    }

    private Ring consumeRing(PartitionName partitionName, HttpStreamResponse got, byte[] intBuffer) {
        try {
            if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                FilerInputStream fis = null;
                try {
                    fis = new FilerInputStream(got.getInputStream());
                    int ringSize = UIO.readInt(fis, "ringSize", intBuffer);
                    int leaderIndex = -1;
                    RingMemberAndHost[] ring = new RingMemberAndHost[ringSize];
                    for (int i = 0; i < ringSize; i++) {
                        byte[] ringMemberBytes = UIO.readByteArray(fis, "ringMember", intBuffer);
                        RingMember ringMember = new RingMember(ringMemberBytes);
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
                    return new Ring(leaderIndex, ring);

                } catch (Exception x) {
                    throw new RuntimeException("Failed loading routes for " + partitionName, x);
                }
            }
        } finally {
            got.close();
        }
        throw new RuntimeException("No routes to partition:" + partitionName + " statusCode:" + got.getStatusCode());
    }
}
