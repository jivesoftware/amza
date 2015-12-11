package com.jivesoftware.os.amza.client.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * @author jonathan.colt
 */
public class PartitionHostsProvider {

    private final TenantAwareHttpClient<String> tenantAwareHttpClient;
    private final ObjectMapper mapper;

    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public PartitionHostsProvider(TenantAwareHttpClient<String> tenantAwareHttpClient, ObjectMapper mapper) {
        this.tenantAwareHttpClient = tenantAwareHttpClient;
        this.mapper = mapper;
    }

    void ensurePartition(PartitionName partitionName, int ringSize, PartitionProperties partitionProperties) throws Exception {
        String base64PartitionName = partitionName.toBase64();
        String partitionPropertiesString = mapper.writeValueAsString(partitionProperties);

        tenantAwareHttpClient.call("", roundRobinStrategy, "ensurePartition", (client) -> {

            HttpResponse got = client.postJson("/amza/v1/ensurePartition/" + base64PartitionName + "/" + ringSize,
                partitionPropertiesString, null);

            if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                return new ClientCall.ClientResponse<>(null, true);
            }
            throw new RuntimeException("Failed to ensure partition:" + partitionName);
        });
    }

    Ring getPartitionHosts(PartitionName partitionName, Optional<RingMemberAndHost> useHost, long waitForLeaderElection) throws HttpClientException {

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
            throw new RuntimeException("No routes to partition:" + partitionName);
        });
    }

    private static final Random RANDOM = new Random();  // Random number generator

    public static class Ring {

        private final int leaderIndex;
        private final RingMemberAndHost[] members;

        public Ring(int leaderIndex, RingMemberAndHost[] members) {
            this.leaderIndex = leaderIndex;
            this.members = members;
        }

        public RingMemberAndHost leader() {
            return (leaderIndex == -1) ? null : members[leaderIndex];
        }

        public RingMemberAndHost[] actualRing() {
            return members;
        }

        public RingMemberAndHost[] orderedRing(List<RingMember> membersInOrder) {
            Map<RingMember, RingMemberAndHost> memberHosts = Maps.uniqueIndex(Arrays.asList(members), input -> input.ringMember);
            RingMemberAndHost[] ordered = new RingMemberAndHost[membersInOrder.size()];
            for (int i = 0; i < membersInOrder.size(); i++) {
                ordered[i] = memberHosts.get(membersInOrder.get(i));
            }
            return ordered;
        }

        public RingMemberAndHost[] leaderlessRing() {
            if (leaderIndex == -1) {
                return Arrays.copyOf(members, members.length);
            } else {
                RingMemberAndHost[] memberAndHosts = new RingMemberAndHost[members.length - 1];
                System.arraycopy(members, 0, memberAndHosts, 0, leaderIndex);
                System.arraycopy(members, leaderIndex + 1, memberAndHosts, leaderIndex, members.length - 1 - leaderIndex);
                return memberAndHosts;
            }
        }

        public RingMemberAndHost[] randomizeRing() {
            RingMemberAndHost[] array = Arrays.copyOf(members, members.length);
            for (int i = 0; i < array.length; i++) {
                int randomPosition = RANDOM.nextInt(array.length);
                RingMemberAndHost temp = array[i];
                array[i] = array[randomPosition];
                array[randomPosition] = temp;
            }
            return array;
        }
    }

}
