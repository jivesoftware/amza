package com.jivesoftware.os.amza.client.http;

import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.client.http.PartitionHostsProvider.Ring;
import com.jivesoftware.os.amza.client.http.exceptions.LeaderElectionInProgressException;
import com.jivesoftware.os.amza.client.http.exceptions.NoLongerTheLeaderException;
import com.jivesoftware.os.amza.client.http.exceptions.NotSolveableException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
 * @author jonathan.colt
 */
public class AmzaHttpClientCallRouter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService callerThreads;
    private final PartitionHostsProvider partitionHostsProvider;
    private final RingHostHttpClientProvider clientProvider;
    private final Cache<PartitionName, Ring> partitionRoutingCache;

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

    public <R, A extends Closeable> R write(List<String> solutionLog,
        PartitionName partitionName,
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
                if (solutionLog != null) {
                    solutionLog.add("Writing to " + leader);
                }
                return solve(solutionLog, partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonAfterNMillis,
                    leader.ringMember, leader);
            } catch (LeaderElectionInProgressException | NoLongerTheLeaderException e) {
                LOG.inc("reattempts>write>" + e.getClass().getSimpleName() + ">" + consistency.name() + ">" + partitionName.toBase64());
                ring = ring(partitionName, consistency, Optional.of(ring.leader()), waitForLeaderElection);
                RingMemberAndHost leader = ring.leader();
                if (solutionLog != null) {
                    solutionLog.add("Leader changed. Reattempting WRITE against " + leader);
                }
                return solve(solutionLog, partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonAfterNMillis,
                    leader.ringMember, leader);
            }
        } else if (consistency == Consistency.quorum
            || consistency == Consistency.write_all_read_one
            || consistency == Consistency.write_one_read_all
            || consistency == Consistency.none) {
            return solve(solutionLog, partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonAfterNMillis, null,
                ring.randomizeRing());
        } else {
            throw new IllegalStateException("Unsupported write consistency:" + consistency.name());
        }

    }

    public void invalidateRouting(PartitionName partitionName) {
        partitionRoutingCache.invalidate(partitionName);
    }

    public <R, A extends Closeable> R read(List<String> solutionLog,
        PartitionName partitionName,
        Consistency consistency,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> call,
        Merger<R, A> merger) throws Exception {

        long waitForLeaderElection = 1000; // TODO config?
        Ring ring = ring(partitionName, consistency, Optional.empty(), waitForLeaderElection);

        long additionalSolverAfterNMillis = 1000; // TODO who controls this

        if (consistency.requiresLeader()) {
            RingMemberAndHost leader;
            try {
                leader = ring.leader();
                A answer;
                try {
                    if (solutionLog != null) {
                        solutionLog.add("Reading from " + leader);
                    }
                    answer = clientProvider.call(partitionName, leader.ringMember, leader, family, call);
                } catch (LeaderElectionInProgressException | NoLongerTheLeaderException e) {
                    LOG.inc("reattempts>read>" + e.getClass().getSimpleName() + ">" + consistency.name() + ">" + partitionName.toBase64());
                    ring = ring(partitionName, consistency, Optional.of(ring.leader()), waitForLeaderElection);
                    leader = ring.leader();
                    if (solutionLog != null) {
                        solutionLog.add("Leader changed. Reattempting READ against " + leader);
                    }
                    answer = clientProvider.call(partitionName, leader.ringMember, leader, family, call);
                }
                R merge = merger.merge(Collections.singletonList(new RingMemberAndHostAnswer<>(leader, answer)));
                return merge;

            } catch (Exception x) {
                partitionRoutingCache.invalidate(partitionName);
                if (consistency == Consistency.leader) {
                    LOG.error("Failed to read from leader.", x);
                    throw x;
                } else {
                    LOG.inc("failover>read>" + consistency.name() + ">" + partitionName.toBase64());
                    LOG.warn("Failed to read from leader.", x);
                }
            }

            if (consistency == Consistency.leader_plus_one) {
                RingMemberAndHost[] leaderlessRing = ring.leaderlessRing();
                if (solutionLog != null) {
                    solutionLog.add("Failing over READ to all " + leaderlessRing.length + " members.");
                }
                return solve(solutionLog,
                    partitionName,
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
                if (solutionLog != null) {
                    solutionLog.add("Failing over READ to " + neighborQuorum + " out of" + leaderlessRing.length + " members.");
                }
                return solve(solutionLog,
                    partitionName,
                    family,
                    call,
                    neighborQuorum,
                    true,
                    merger,
                    additionalSolverAfterNMillis,
                    Long.MAX_VALUE,
                    null,
                    leaderlessRing);
            } else if (consistency == Consistency.leader_all) {
                RingMemberAndHost[] leaderlessRing = ring.leaderlessRing();
                if (solutionLog != null) {
                    solutionLog.add("Failing over READ to 1 out of" + leaderlessRing.length + " members.");
                }
                return solve(solutionLog,
                    partitionName,
                    family,
                    call,
                    1,
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
            return solve(solutionLog, partitionName, family, call, 1 + neighborQuorum, true, merger, additionalSolverAfterNMillis, Long.MAX_VALUE,
                null, randomizeRing);
        } else if (consistency == Consistency.write_one_read_all) {
            RingMemberAndHost[] actualRing = ring.actualRing();
            return solve(solutionLog, partitionName, family, call, actualRing.length, false, merger, additionalSolverAfterNMillis, Long.MAX_VALUE,
                null, actualRing);
        } else if (consistency == Consistency.write_all_read_one) {
            RingMemberAndHost[] randomizeRing = ring.randomizeRing();
            return solve(solutionLog, partitionName, family, call, 1, true, merger, additionalSolverAfterNMillis, Long.MAX_VALUE, null,
                randomizeRing);
        } else if (consistency == Consistency.none) {
            RingMemberAndHost[] randomizeRing = ring.randomizeRing();
            return solve(solutionLog, partitionName, family, call, 1, true, merger, additionalSolverAfterNMillis, Long.MAX_VALUE, null,
                randomizeRing);
        } else {
            throw new IllegalStateException("Unsupported read consistency:" + consistency.name());
        }
    }

    public <R, A extends Closeable> R take(List<String> solutionLog,
        PartitionName partitionName,
        List<RingMember> membersInOrder,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> call,
        Merger<R, A> merger) throws Exception {

        long waitForLeaderElection = 1000; // TODO config?
        Ring ring = ring(partitionName, Consistency.none, Optional.empty(), waitForLeaderElection);

        long additionalSolverAfterNMillis = 1000; // TODO who controls this

        RingMemberAndHost[] orderedRing = ring.orderedRing(membersInOrder);
        return solve(solutionLog, partitionName, family, call, 1, true, merger, additionalSolverAfterNMillis, Long.MAX_VALUE, null, orderedRing);
    }

    private Ring ring(PartitionName partitionName,
        Consistency consistency,
        Optional<RingMemberAndHost> useHost,
        long waitForLeaderElection) throws HttpClientException, ExecutionException,
        LeaderElectionInProgressException {

        Ring ring = partitionRoutingCache.getIfPresent(partitionName);
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

    private <R, A extends Closeable> R solve(List<String> solutionLog,
        PartitionName partitionName,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> partitionCall,
        int mandatory,
        boolean addNewSolverOnTimeout,
        Merger<R, A> merger,
        long addAdditionalSolverAfterNMillis,
        long abandonAfterNMillis,
        RingMember leader,
        RingMemberAndHost... ringMemberAndHosts) throws Exception {
        long start = System.currentTimeMillis();
        List<Closeable> closeables = Collections.synchronizedList(Lists.newArrayListWithCapacity(mandatory));
        try {
            if (solutionLog != null) {
                solutionLog.add("Solving...");
                solutionLog.add("family:" + family);
                solutionLog.add("partitionName:" + partitionName);
                solutionLog.add("mandatory:" + mandatory);
                solutionLog.add("addNewSolverOnTimeout:" + addNewSolverOnTimeout);
                solutionLog.add("addAdditionalSolverAfterNMillis:" + addAdditionalSolverAfterNMillis);
                solutionLog.add("abandonAfterNMillis:" + abandonAfterNMillis);
            }

            Iterable<Callable<RingMemberAndHostAnswer<A>>> callOrder = Iterables.transform(
                Iterables.filter(Arrays.asList(ringMemberAndHosts), Predicates.notNull()),
                (ringMemberAndHost) -> {
                    if (solutionLog != null) {
                        solutionLog.add("Adding solver " + ringMemberAndHost);
                    }
                    return () -> {
                        A answer = clientProvider.call(partitionName, leader, ringMemberAndHost, family, partitionCall);
                        closeables.add(answer);
                        return new RingMemberAndHostAnswer<>(ringMemberAndHost, answer);
                    };
                });
            List<RingMemberAndHostAnswer<A>> solutions = solve(solutionLog, callerThreads, callOrder.iterator(), mandatory,
                addNewSolverOnTimeout, addAdditionalSolverAfterNMillis, abandonAfterNMillis);
            R result = merger.merge(solutions);
            if (solutionLog != null) {
                solutionLog.add("Solved. " + (System.currentTimeMillis() - start) + "millis");
            }
            return result;
        } catch (NotSolveableException nse) {
            LOG.inc("notSolveable>" + partitionName.toBase64());
            partitionRoutingCache.invalidate(partitionName);
            if (solutionLog != null) {
                solutionLog.add("Not solvable. " + (System.currentTimeMillis() - start) + "millis");
            }
            throw nse;
        } catch (Throwable t) {
            if (solutionLog != null) {
                solutionLog.add("Failed to solve." + t + " " + (System.currentTimeMillis() - start) + "millis");
            }
            throw t;
        } finally {
            for (Closeable closeable : closeables) {
                try {
                    closeable.close();
                } catch (Throwable t) {
                    LOG.warn("Failed to close {}", new Object[] { closeable }, t);
                }
            }
        }
    }

    private <A> List<RingMemberAndHostAnswer<A>> solve(List<String> solutionLog,
        Executor executor,
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
                    if (solutionLog != null) {
                        solutionLog.add("Not enough solveable available. desire:" + mandatory);
                    }
                    throw new NotSolveableException("Not enough solveable available. desire:" + mandatory);
                }
            }
            long start = System.currentTimeMillis();
            while (answers.size() < mandatory && (solvers.hasNext() || pending > 0)) {
                long elapsed = System.currentTimeMillis() - start;
                long remaining = abandonAfterNMillis - elapsed;
                if (remaining <= 0) {
                    if (solutionLog != null) {
                        solutionLog.add("Abandoned solution because it took more than " + abandonAfterNMillis + "millis.");
                    }
                    throw new RuntimeException("Abandoned solution because it took more than " + abandonAfterNMillis + "millis.");
                }
                Future<RingMemberAndHostAnswer<A>> future = completionService.poll(Math.min(remaining, addAdditionalSoverAfterNMillis), TimeUnit.MILLISECONDS);
                if (future == null) {
                    if (addNewSolverOnTimeout) {
                        if (solvers.hasNext()) {
                            pending++;
                            Callable<RingMemberAndHostAnswer<A>> next = solvers.next();
                            futures.add(completionService.submit(next));
                        }
                    }
                } else {
                    try {
                        RingMemberAndHostAnswer<A> r = future.get();
                        if (r != null) {
                            if (solutionLog != null) {
                                solutionLog.add("solving with " + r.getRingMemberAndHost());
                            }
                            answers.add(r);
                        }
                    } catch (ExecutionException ignore) {
                        if (solutionLog != null) {
                            solutionLog.add("solver faild to with " + ignore.getCause());
                        }
                        LOG.warn("Failed to solve", ignore.getCause());
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
