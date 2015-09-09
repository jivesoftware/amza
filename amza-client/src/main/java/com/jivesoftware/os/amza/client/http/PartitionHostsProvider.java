package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

/**
 *
 * @author jonathan.colt
 */
public class PartitionHostsProvider {

    private final TenantAwareHttpClient<String> tenantAwareHttpClient;

    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public PartitionHostsProvider(TenantAwareHttpClient<String> tenantAwareHttpClient) {
        this.tenantAwareHttpClient = tenantAwareHttpClient;
    }

    Ring getPartitionHosts(PartitionName partitionName, Optional<RingMemberAndHost> useHost, long waitForLeaderElection) throws HttpClientException {

        NextClientStrategy strategy = useHost.map((value) -> (NextClientStrategy)new ConnectionDescriptorSelectiveStrategy(new HostPort[]{
            new HostPort(value.ringHost.getHost(), value.ringHost.getPort())
        })).orElse(roundRobinStrategy);

        return tenantAwareHttpClient.call("", strategy, "getPartitionHosts", (client) -> {

            HttpStreamResponse got = client.streamingPost("/amza/v1/ring/"
                + partitionName.toBase64() + "/" + waitForLeaderElection, null, null);
            if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
                try {
                    FilerInputStream fis = new FilerInputStream(got.getInputStream());
                    int ringSize = UIO.readInt(fis, "ringSize");
                    int leaderIndex = -1;
                    RingMemberAndHost[] ring = new RingMemberAndHost[ringSize];
                    for (int i = 0; i < ringSize; i++) {
                        RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(fis, "ringMember"));
                        RingHost ringHost = RingHost.fromBytes(UIO.readByteArray(fis, "ringHost"));
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
            throw new RuntimeException("No routes to partition:" + partitionName);
        });
    }

    private static final Random RANDOM = new Random();  // Random number generator

    public static class Ring {

        private final int leaderIndex;
        private final RingMemberAndHost[] members;
        private RingMemberAndHost[] leaderLessMembers;

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

        public RingMemberAndHost[] leaderlessRing() {
            if (leaderLessMembers != null) {
                return Arrays.copyOf(leaderLessMembers, leaderLessMembers.length);
            } else if (leaderIndex == -1) {
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

    public static class RingMemberAndHost {

        public final RingMember ringMember;
        public final RingHost ringHost;

        public RingMemberAndHost(RingMember ringMember, RingHost ringHost) {
            this.ringMember = ringMember;
            this.ringHost = ringHost;
        }
    }
}

/*
 UIO.writeInt(cf, memebers.size(), "ringSize");
 for (Map.Entry<RingMember, RingHost> r : memebers) {
 UIO.writeByteArray(cf, r.getKey().toBytes(), "ringMember");
 UIO.writeByteArray(cf, r.getValue().toBytes(), "ringMember");
 boolean isLeader = leader != null && Arrays.equals(r.getKey().toBytes(), leader.toBytes());
 UIO.writeBoolean(cf, isLeader, "leader");
 }
 */
