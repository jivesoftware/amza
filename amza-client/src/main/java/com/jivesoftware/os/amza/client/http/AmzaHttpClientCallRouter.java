package com.jivesoftware.os.amza.client.http;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.client.http.PartitionHostsProvider.Ring;
import com.jivesoftware.os.amza.client.http.PartitionHostsProvider.RingMemberAndHost;
import com.jivesoftware.os.amza.client.http.exceptions.LeaderElectionInProgressException;
import com.jivesoftware.os.amza.client.http.exceptions.NoLongerTheLeaderException;
import com.jivesoftware.os.amza.client.http.exceptions.NotSolveableException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class AmzaHttpClientCallRouter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionHostsProvider partitionHostsProvider;
    private final Cache<PartitionName, Ring> partitionRoutingCache;
    private final RingHostHttpClientProvider clientProvider;
    private final ExecutorService callerThreads;

    public AmzaHttpClientCallRouter(ExecutorService callerThreads,
        PartitionHostsProvider partitionHostsProvider,
        RingHostHttpClientProvider clientProvider) {
        this.callerThreads = callerThreads;
        this.partitionHostsProvider = partitionHostsProvider;
        this.clientProvider = clientProvider;
        this.partitionRoutingCache = CacheBuilder.newBuilder()
            .maximumSize(50_000) //TODO config
            .expireAfterWrite(5, TimeUnit.MINUTES) //TODO config
            .build();
    }

    public <R, A> R write(PartitionName partitionName,
        Consistency consistency,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> partitionCall,
        Merger<R, A> merger,
        long abandonAfterNMillis) throws Exception {

        long waitForLeaderElection = 1000; // TODO config?
        Ring ring = ring(partitionName, consistency, Optional.empty(), waitForLeaderElection);

        long additionalSolverAfterNMillis = 1000; // TODO who controls this
        if (consistency.requiresLeader()) {
            try {
                RingMemberAndHost leader = ring.leader();
                return solve(partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonAfterNMillis, leader.ringMember,
                    leader);
            } catch (LeaderElectionInProgressException | NoLongerTheLeaderException e) {
                ring = ring(partitionName, consistency, Optional.of(ring.leader()), waitForLeaderElection);
                RingMemberAndHost leader = ring.leader();
                return solve(partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonAfterNMillis, leader.ringMember,
                    leader);
            }
        } else if (consistency == Consistency.quorum || consistency == Consistency.write_all_read_one) {
            return solve(partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonAfterNMillis, null, ring.randomizeRing());
        } else {
            throw new IllegalStateException("Unsupported write consistency:" + consistency.name());
        }

    }

    public void invalidateRouting(PartitionName partitionName) {
        partitionRoutingCache.invalidate(partitionName);
    }

    public <R, A> R read(PartitionName partitionName,
        Consistency consistency,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> call,
        Merger<R, A> merger) throws Exception {

        long waitForLeaderElection = 1000; // TODO config?
        Ring ring = ring(partitionName, consistency, Optional.empty(), waitForLeaderElection);

        long additionalSolverAfterNMillis = 1000; // TODO who controls this

        if (consistency.requiresLeader()) {
            RingMemberAndHost leader = null;
            try {
                leader = ring.leader();
                A answer;
                try {
                    answer = clientProvider.call(partitionName, leader.ringMember, leader, family, call);
                } catch (LeaderElectionInProgressException | NoLongerTheLeaderException e) {
                    ring = ring(partitionName, consistency, Optional.of(ring.leader()), waitForLeaderElection);
                    leader = ring.leader();
                    answer = clientProvider.call(partitionName, leader.ringMember, leader, family, call);
                }
                return merger.merge(Arrays.asList(new RingMemberAndHostAnswer<>(leader, answer)));

            } catch (Exception x) {
                partitionRoutingCache.invalidate(partitionName);
                if (consistency == Consistency.leader) {
                    LOG.error("Failed to read from leader.", x);
                    throw x;
                } else {
                    LOG.warn("Failed to read from leader.", x);
                }
            }

            if (consistency == Consistency.leader_plus_one) {
                RingMemberAndHost[] leaderlessRing = ring.leaderlessRing();
                return solve(partitionName,
                    family,
                    call,
                    leaderlessRing.length,
                    false,
                    merger,
                    additionalSolverAfterNMillis,
                    Long.MAX_VALUE,
                    null,
                    leaderlessRing);
            } else if (consistency == Consistency.leader_quorum) {
                RingMemberAndHost[] leaderlessRing = ring.leaderlessRing();
                int neighborQuorum = consistency.quorum(leaderlessRing.length);
                return solve(partitionName,
                    family,
                    call,
                    neighborQuorum,
                    true,
                    merger,
                    additionalSolverAfterNMillis,
                    Long.MAX_VALUE,
                    null,
                    leaderlessRing);
            } else {
                throw new RuntimeException("Unsupported leader read consistency:" + consistency);
            }
        } else if (consistency == Consistency.quorum) {
            RingMemberAndHost[] randomizeRing = ring.randomizeRing();
            int neighborQuorum = consistency.quorum(randomizeRing.length - 1);
            return solve(partitionName, family, call, 1 + neighborQuorum, true, merger, additionalSolverAfterNMillis, Long.MAX_VALUE, null, randomizeRing);
        } else if (consistency == Consistency.write_one_read_all) {
            RingMemberAndHost[] actualRing = ring.actualRing();
            return solve(partitionName, family, call, actualRing.length, false, merger, additionalSolverAfterNMillis, Long.MAX_VALUE, null, actualRing);
        } else {
            throw new IllegalStateException("Unsupported read consistency:" + consistency.name());
        }
    }

    private Ring ring(PartitionName partitionName,
        Consistency consistency,
        Optional<RingMemberAndHost> useHost,
        long waitForLeaderElection) throws HttpClientException, ExecutionException,
        LeaderElectionInProgressException {

        Ring ring = partitionRoutingCache.get(partitionName, null);
        if (ring == null || consistency.requiresLeader() && ring.leader() == null) {
            ring = partitionHostsProvider.getPartitionHosts(partitionName, useHost, waitForLeaderElection);
            if (consistency.requiresLeader()) {
                if (ring.leader() == null) {
                    throw new LeaderElectionInProgressException("It took to long for a leader to be elected.");
                }
            }
            partitionRoutingCache.put(partitionName, ring);
        }
        return ring;
    }

    private <R, A> R solve(PartitionName partitionName,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> partitionCall,
        int mandatory,
        boolean addNewSolverOnTimeout,
        Merger<R, A> merger,
        long addAdditionalSolverAfterNMillis,
        long abandonAfterNMillis,
        RingMember leader,
        RingMemberAndHost... ringMemberAndHosts) throws InterruptedException, Exception {
        try {
            Iterable<Callable<RingMemberAndHostAnswer<A>>> callOrder = Iterables.transform(Arrays.asList(ringMemberAndHosts),
                (ringMemberAndHost) -> () -> {
                    A answer = clientProvider.call(partitionName, leader, ringMemberAndHost, family, partitionCall);
                    return new RingMemberAndHostAnswer<>(ringMemberAndHost, answer);
                });
            List<RingMemberAndHostAnswer<A>> solutions = solve(callerThreads, callOrder.iterator(), mandatory,
                addNewSolverOnTimeout, addAdditionalSolverAfterNMillis, abandonAfterNMillis);
            return merger.merge(solutions);
        } catch (NotSolveableException nse) {
            partitionRoutingCache.invalidate(partitionName);
            throw nse;
        }
    }

    private <A> List<RingMemberAndHostAnswer<A>> solve(Executor executor,
        Iterator<Callable<RingMemberAndHostAnswer<A>>> solvers,
        int mandatory,
        boolean addNewSolverOnTimeout,
        long addAdditionalSoverAfterNMillis,
        long abandonAfterNMillis) throws InterruptedException {

        CompletionService<RingMemberAndHostAnswer<A>> completionService = new ExecutorCompletionService<>(executor);
        List<Future<RingMemberAndHostAnswer<A>>> futures = new ArrayList<>();
        List<RingMemberAndHostAnswer<A>> answers = new ArrayList<>();
        try {
            int pending = 0;
            for (int i = 0; i < mandatory; i++) {
                if (solvers.hasNext()) {
                    pending++;
                    futures.add(completionService.submit(solvers.next()));
                } else {
                    throw new NotSolveableException("Not enough solveable available. desire:" + mandatory);
                }
            }
            long start = System.currentTimeMillis();
            while (answers.size() < mandatory && (solvers.hasNext() || pending > 0)) {
                long elapsed = System.currentTimeMillis() - start;
                long remaining = abandonAfterNMillis - elapsed;
                if (remaining <= 0) {
                    throw new RuntimeException("Abandoned solution because it took more than " + abandonAfterNMillis + "millis.");
                }
                Future<RingMemberAndHostAnswer<A>> future = completionService.poll(Math.min(remaining, addAdditionalSoverAfterNMillis), TimeUnit.MILLISECONDS);
                if (future == null) {
                    if (addNewSolverOnTimeout) {
                        if (solvers.hasNext()) {
                            pending++;
                            futures.add(completionService.submit(solvers.next()));
                        }
                    }
                } else {
                    try {
                        RingMemberAndHostAnswer<A> r = future.get();
                        if (r != null) {
                            answers.add(r);
                        }
                    } catch (ExecutionException ignore) {
                        if (solvers.hasNext()) {
                            pending++;
                            futures.add(completionService.submit(solvers.next()));
                        }
                    } finally {
                        pending--;
                    }
                }
            }
            if (answers.size() != mandatory) {
                throw new NotSolveableException("Not currently solveable. desire:" + mandatory + " achieved:" + answers.size());
            }
        } finally {
            for (Future<RingMemberAndHostAnswer<A>> f : futures) {
                f.cancel(true);
            }
        }
        return answers;
    }

}
