package com.jivesoftware.os.amza.client.http;

import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.client.http.exceptions.LeaderElectionInProgressException;
import com.jivesoftware.os.amza.client.http.exceptions.NoLongerTheLeaderException;
import com.jivesoftware.os.amza.client.http.exceptions.NotSolveableException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class AmzaClientCallRouter<C, E extends Throwable> implements RouteInvalidator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService callerThreads;
    private final PartitionHostsProvider partitionHostsProvider;
    private final RingHostClientProvider<C, E> clientProvider;
    private final Cache<PartitionName, Ring> partitionRoutingCache;

    private final ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("amza-client-cleanup-%d").build());

    public AmzaClientCallRouter(ExecutorService callerThreads,
        PartitionHostsProvider partitionHostsProvider,
        RingHostClientProvider<C, E> clientProvider) {
        this.callerThreads = callerThreads;
        this.partitionHostsProvider = partitionHostsProvider;
        this.clientProvider = clientProvider;
        this.partitionRoutingCache = CacheBuilder.newBuilder()
            .maximumSize(50_000) //TODO config
            .expireAfterWrite(5, TimeUnit.MINUTES) //TODO config
            .build();
    }

    public <R, A extends Abortable> R write(List<String> solutionLog,
        PartitionName partitionName,
        Consistency consistency,
        String family,
        PartitionCall<C, A, E> partitionCall,
        Merger<R, A> merger,
        long awaitLeaderElectionForNMillis,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis) throws Exception {

        Ring ring = ring(partitionName, consistency, Optional.empty(), awaitLeaderElectionForNMillis);

        if (consistency.requiresLeader()) {
            try {
                RingMemberAndHost leader = ring.leader();
                if (solutionLog != null) {
                    solutionLog.add("Writing to " + leader);
                }
                return solve(solutionLog, partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis,
                    leader.ringMember, leader);
            } catch (LeaderElectionInProgressException | NoLongerTheLeaderException | ExecutionException e) {
                LOG.inc("reattempts>write>" + e.getClass().getSimpleName() + ">" + consistency.name());
                partitionRoutingCache.invalidate(partitionName);
                ring = ring(partitionName,
                    consistency,
                    (e instanceof ExecutionException) ? Optional.empty() : Optional.of(ring.leader()),
                    awaitLeaderElectionForNMillis);
                RingMemberAndHost leader = ring.leader();
                if (solutionLog != null) {
                    solutionLog.add("Leader may have changed. Reattempting WRITE against " + leader);
                }
                return solve(solutionLog, partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis,
                    leader.ringMember, leader);
            }
        } else if (consistency == Consistency.quorum
            || consistency == Consistency.write_all_read_one
            || consistency == Consistency.write_one_read_all
            || consistency == Consistency.none) {
            return solve(solutionLog, partitionName, family, partitionCall, 1, false, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis, null,
                ring.randomizeRing());
        } else {
            throw new IllegalStateException("Unsupported write consistency:" + consistency.name());
        }

    }

    @Override
    public void invalidateRouting(PartitionName partitionName) {
        partitionRoutingCache.invalidate(partitionName);
    }

    public <R, A extends Abortable> R read(List<String> solutionLog,
        PartitionName partitionName,
        Consistency consistency,
        String family,
        PartitionCall<C, A, E> call,
        Merger<R, A> merger,
        long awaitLeaderElectionForNMillis,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis) throws Exception {

        Ring ring = ring(partitionName, consistency, Optional.empty(), awaitLeaderElectionForNMillis);

        if (consistency.requiresLeader()) {
            Future<A> future = null;
            boolean closeable = false;
            AtomicReference<Abortable> abortableRef = new AtomicReference<>();
            RingMemberAndHost leader = ring.leader();
            try {
                A answer = null;
                try {
                    RingMemberAndHost initialLeader = leader;
                    if (solutionLog != null) {
                        solutionLog.add("Reading from " + initialLeader);
                    }
                    future = callerThreads.submit(() -> {
                        A clientAnswer = clientProvider.call(partitionName, initialLeader.ringMember, initialLeader, family, call);
                        abortableRef.set(clientAnswer);
                        return clientAnswer;
                    });
                    answer = future.get(abandonLeaderSolutionAfterNMillis, TimeUnit.MILLISECONDS);
                    closeable = true;
                } catch (LeaderElectionInProgressException | NoLongerTheLeaderException | ExecutionException e) {
                    LOG.inc("reattempts>read>" + e.getClass().getSimpleName() + ">" + consistency.name());
                    partitionRoutingCache.invalidate(partitionName);
                    ring = ring(partitionName,
                        consistency,
                        (e instanceof ExecutionException) ? Optional.empty() : Optional.of(ring.leader()),
                        awaitLeaderElectionForNMillis);
                    leader = ring.leader();
                    RingMemberAndHost nextLeader = leader;
                    if (solutionLog != null) {
                        solutionLog.add("Leader may have changed. Reattempting READ against " + leader);
                    }
                    if (future != null) {
                        future.cancel(true);
                    }
                    Abortable abortable = abortableRef.getAndSet(null);
                    if (abortable != null) {
                        try {
                            abortable.close();
                        } catch (Throwable t) {
                            LOG.warn("Failed to close: {}: {}", t.getClass().getSimpleName(), t.getMessage());
                        }
                    }
                    future = callerThreads.submit(() -> {
                        A clientAnswer = clientProvider.call(partitionName, nextLeader.ringMember, nextLeader, family, call);
                        abortableRef.set(clientAnswer);
                        return clientAnswer;
                    });
                    answer = future.get(abandonLeaderSolutionAfterNMillis, TimeUnit.MILLISECONDS);
                    closeable = true;
                }
                return merger.merge(Collections.singletonList(new RingMemberAndHostAnswer<>(leader, answer)));
            } catch (TimeoutException x) {
                if (consistency == Consistency.leader) {
                    LOG.error("Timed out reading from leader {} for {}", new Object[] { leader, partitionName }, x);
                    throw x;
                } else {
                    LOG.inc("timeout>read>" + consistency.name());
                    LOG.warn("Timed out reading from leader {} for {}, will retry at quorum", leader, partitionName);
                }
            } catch (IllegalArgumentException x) {
                LOG.error("Illegal argument, there is likely a problem with the request to leader {} for {}", new Object[] { leader, partitionName }, x);
                throw x;
            } catch (Exception x) {
                partitionRoutingCache.invalidate(partitionName);
                if (consistency == Consistency.leader) {
                    LOG.error("Failed to read from leader {} for {}", new Object[] { leader, partitionName }, x);
                    throw x;
                } else {
                    LOG.inc("failover>read>" + consistency.name());
                    LOG.warn("Failed to read from leader {} for {}, will retry at quorum", new Object[] { leader, partitionName }, x);
                }
            } finally {
                if (future != null) {
                    future.cancel(true);
                }
                Abortable abortable = abortableRef.get();
                if (abortable != null) {
                    try {
                        if (closeable) {
                            abortable.close();
                        } else {
                            abortable.abort();
                        }
                    } catch (Throwable t) {
                        LOG.warn("Failed to close: {}: {}", t.getClass().getSimpleName(), t.getMessage());
                    }
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
                    abandonSolutionAfterNMillis,
                    null,
                    leaderlessRing);
            } else if (consistency == Consistency.leader_quorum) {
                RingMemberAndHost[] leaderlessRing = ring.leaderlessRing();
                int neighborQuorum = 1 + consistency.repairQuorum(leaderlessRing.length);
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
                    abandonSolutionAfterNMillis,
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
                    abandonSolutionAfterNMillis,
                    null,
                    leaderlessRing);
            } else {
                throw new RuntimeException("Unsupported leader read consistency:" + consistency);
            }
        } else if (consistency == Consistency.quorum) {
            RingMemberAndHost[] randomizeRing = ring.randomizeRing();
            int neighborQuorum = consistency.quorum(randomizeRing.length - 1);
            return solve(solutionLog, partitionName, family, call, 1 + neighborQuorum, true, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis,
                null, randomizeRing);
        } else if (consistency == Consistency.write_one_read_all) {
            RingMemberAndHost[] actualRing = ring.actualRing();
            return solve(solutionLog, partitionName, family, call, actualRing.length, false, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis,
                null, actualRing);
        } else if (consistency == Consistency.write_all_read_one) {
            RingMemberAndHost[] randomizeRing = ring.randomizeRing();
            return solve(solutionLog, partitionName, family, call, 1, true, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis, null,
                randomizeRing);
        } else if (consistency == Consistency.none) {
            RingMemberAndHost[] randomizeRing = ring.randomizeRing();
            return solve(solutionLog, partitionName, family, call, 1, true, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis, null,
                randomizeRing);
        } else {
            throw new IllegalStateException("Unsupported read consistency:" + consistency.name());
        }
    }

    public <R, A extends Abortable> R take(List<String> solutionLog,
        PartitionName partitionName,
        List<RingMember> membersInOrder,
        String family,
        PartitionCall<C, A, E> call,
        Merger<R, A> merger,
        long awaitLeaderElectionForNMillis,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis) throws Exception {

        Ring ring = ring(partitionName, Consistency.none, Optional.empty(), awaitLeaderElectionForNMillis);

        RingMemberAndHost[] orderedRing = ring.orderedRing(membersInOrder);
        return solve(solutionLog, partitionName, family, call, 1, true, merger, additionalSolverAfterNMillis, abandonSolutionAfterNMillis, null, orderedRing);
    }

    private Ring ring(PartitionName partitionName,
        Consistency consistency,
        Optional<RingMemberAndHost> useHost,
        long waitForLeaderElection) throws Exception {

        Ring ring = partitionRoutingCache.getIfPresent(partitionName);
        if (ring == null || consistency.requiresLeader() && ring.leader() == null) {
            ring = partitionHostsProvider.getPartitionHosts(partitionName, useHost, consistency.requiresLeader() ? waitForLeaderElection : 0);
            if (consistency.requiresLeader()) {
                if (ring.leader() == null) {
                    throw new LeaderElectionInProgressException("It took too long for a leader to be elected.");
                }
            }
            partitionRoutingCache.put(partitionName, ring);
        }
        return ring;
    }

    private <R, A extends Abortable> R solve(List<String> solutionLog,
        PartitionName partitionName,
        String family,
        PartitionCall<C, A, E> partitionCall,
        int mandatory,
        boolean addNewSolverOnTimeout,
        Merger<R, A> merger,
        long addAdditionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        RingMember leader,
        RingMemberAndHost... ringMemberAndHosts) throws Exception {
        long start = System.currentTimeMillis();
        List<Abortable> abortables = Collections.synchronizedList(Lists.newArrayListWithCapacity(mandatory));
        List<Abortable> closeables = Lists.newArrayListWithCapacity(mandatory);
        boolean closeable = false;
        try {
            if (solutionLog != null) {
                solutionLog.add("Solving...");
                solutionLog.add("family:" + family);
                solutionLog.add("partitionName:" + partitionName);
                solutionLog.add("mandatory:" + mandatory);
                solutionLog.add("addNewSolverOnTimeout:" + addNewSolverOnTimeout);
                solutionLog.add("addAdditionalSolverAfterNMillis:" + addAdditionalSolverAfterNMillis);
                solutionLog.add("abandonSolutionAfterNMillis:" + abandonSolutionAfterNMillis);
            }

            Iterable<Callable<RingMemberAndHostAnswer<A>>> callOrder = Iterables.transform(
                Iterables.filter(Arrays.asList(ringMemberAndHosts), Predicates.notNull()),
                (ringMemberAndHost) -> {
                    if (solutionLog != null) {
                        solutionLog.add("Adding solver " + ringMemberAndHost);
                    }
                    return () -> {
                        A answer = clientProvider.call(partitionName, leader, ringMemberAndHost, family, partitionCall);
                        abortables.add(answer);
                        return new RingMemberAndHostAnswer<>(ringMemberAndHost, answer);
                    };
                });
            List<RingMemberAndHostAnswer<A>> solutions = solve(solutionLog, callerThreads, callOrder.iterator(), mandatory,
                addNewSolverOnTimeout, addAdditionalSolverAfterNMillis, abandonSolutionAfterNMillis);
            for (RingMemberAndHostAnswer<A> solution : solutions) {
                closeables.add(solution.getAnswer());
                abortables.remove(solution.getAnswer());
            }
            R result = merger.merge(solutions);
            if (solutionLog != null) {
                solutionLog.add("Solved. " + (System.currentTimeMillis() - start) + "millis");
            }
            closeable = true;
            return result;
        } catch (NotSolveableException nse) {
            LOG.inc("notSolveable");
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
            for (Abortable abortable : abortables) {
                try {
                    abortable.abort();
                } catch (Throwable t) {
                    LOG.warn("Failed to abort {} using leader {} hosts {} for {}",
                        new Object[] { abortable, leader, Arrays.toString(ringMemberAndHosts), partitionName }, t);
                }
            }
            for (Abortable abortable : closeables) {
                try {
                    if (closeable) {
                        abortable.close();
                    } else {
                        abortable.abort();
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to close {} using leader {} hosts {} for {} closeable:{}",
                        new Object[] { abortable, leader, Arrays.toString(ringMemberAndHosts), partitionName, closeable }, t);
                }
            }
        }
    }

    private <A extends Abortable> List<RingMemberAndHostAnswer<A>> solve(List<String> solutionLog,
        Executor executor,
        Iterator<Callable<RingMemberAndHostAnswer<A>>> solvers,
        int mandatory,
        boolean addNewSolverOnTimeout,
        long addAdditionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis) throws InterruptedException {

        List<Future<RingMemberAndHostAnswer<A>>> futures = new ArrayList<>();
        List<RingMemberAndHostAnswer<A>> answers = new ArrayList<>();
        try {
            CompletionService<RingMemberAndHostAnswer<A>> completionService = new ExecutorCompletionService<>(executor);
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
                long remaining = abandonSolutionAfterNMillis - elapsed;
                if (remaining <= 0) {
                    if (solutionLog != null) {
                        solutionLog.add("Abandoned solution because it took more than " + abandonSolutionAfterNMillis + "millis.");
                    }
                    throw new RuntimeException("Abandoned solution because it took more than " + abandonSolutionAfterNMillis + "millis.");
                }
                Future<RingMemberAndHostAnswer<A>> future = completionService.poll(Math.min(remaining, addAdditionalSolverAfterNMillis), TimeUnit.MILLISECONDS);
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
                                solutionLog.add("Solving with " + r.getRingMemberAndHost());
                            }
                            answers.add(r);
                        }
                    } catch (ExecutionException ignore) {
                        if (solutionLog != null) {
                            solutionLog.add("Solver failed: " + ignore.getCause());
                        }
                        LOG.debug("Failed to solve", ignore.getCause());
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
            for (Future<RingMemberAndHostAnswer<A>> future : futures) {
                future.cancel(true);
            }
        }
        return answers;
    }

}
