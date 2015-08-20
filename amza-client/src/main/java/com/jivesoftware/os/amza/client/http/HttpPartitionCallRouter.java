package com.jivesoftware.os.amza.client.http;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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
public class HttpPartitionCallRouter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final PartitionHostsProvider partitionHostsProvider;
    private final Cache<PartitionName, RingHost[]> partitionRoutingCache;
    private final HttpClientProvider clientProvider;
    private final ExecutorService callerThreads;

    public HttpPartitionCallRouter(ExecutorService callerThreads,
        PartitionHostsProvider partitionHostsProvider,
        HttpClientProvider clientProvider) {
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
        Merger<R, A> merger) throws Exception {

        RingHost[] ringHosts = partitionRoutingCache.get(partitionName, () -> {
            RingHost[] partitionRingHosts = partitionHostsProvider.getPartitionHosts(partitionName);
            if (partitionRingHosts == null || partitionRingHosts.length == 0) {
                throw new RuntimeException("No routes to partition:" + partitionName);
            }
            return partitionRingHosts;
        });

        if (consistency == Consistency.leader || consistency == Consistency.leader_plus_one || consistency == Consistency.leader_quorum) {
            // TODO handle failures due to leader re-election and retry.
            return solve(new RingHost[]{ringHosts[0]}, partitionName, family, partitionCall, 1, merger);
        } else if (consistency == Consistency.quorum) {
            randomizeArray(ringHosts);
            return solve(ringHosts, partitionName, family, partitionCall, 1, merger);
        } else if (consistency == Consistency.write_all_read_one) {
            return solve(ringHosts, partitionName, family, partitionCall, ringHosts.length, merger);
        } else {
            throw new IllegalStateException("Unsupported write consistency:" + consistency.name());
        }

    }

    public <R, A> R read(PartitionName partitionName,
        Consistency consistency,
        String family,
        PartitionCall<HttpClient, A, HttpClientException> partitionCall,
        Merger<R, A> merger) throws Exception {

        RingHost[] ringHosts = partitionRoutingCache.get(partitionName, () -> {
            RingHost[] partitionRingHosts = partitionHostsProvider.getPartitionHosts(partitionName);
            if (partitionRingHosts == null || partitionRingHosts.length == 0) {
                throw new RuntimeException("No routes to partition:" + partitionName);
            }
            return partitionRingHosts;
        });

        if (consistency == Consistency.leader) {
            try {
                // TODO handle failures due to leader re-election and retry.
                return merger.merge(Arrays.asList(new RingHostAnswer<>(ringHosts[0], clientProvider.call(partitionName, ringHosts[0], family, partitionCall))));
            } catch (Exception x) {
                partitionRoutingCache.invalidate(partitionName);
                LOG.warn("Failed to read from leader.", x);
                throw x;
            }
        } else if (consistency == Consistency.leader_plus_one) {
            try {
                // TODO handle failures due to leader re-election and retry.
                return merger.merge(Arrays.asList(new RingHostAnswer<>(ringHosts[0], clientProvider.call(partitionName, ringHosts[0], family, partitionCall))));
            } catch (Exception x) {
                partitionRoutingCache.invalidate(partitionName);
                LOG.warn("Failed to read from leader. Falling back on read all.");
            }
            int l = ringHosts.length - 1;
            RingHost[] newRingHosts = new RingHost[l];
            System.arraycopy(ringHosts, 1, newRingHosts, 0, l);
            return solve(newRingHosts, partitionName, family, partitionCall, newRingHosts.length, merger);
        } else if (consistency == Consistency.leader_quorum) {
            try {
                // TODO handle failures due to leader re-election and retry.
                return merger.merge(Arrays.asList(new RingHostAnswer<>(ringHosts[0], clientProvider.call(partitionName, ringHosts[0], family, partitionCall))));
            } catch (Exception x) {
                partitionRoutingCache.invalidate(partitionName);
                LOG.warn("Failed to read from leader. Falling back on read quorum.");
            }
            int neighborQuorum = consistency.quorum(ringHosts.length - 1);
            int l = ringHosts.length - 1;
            RingHost[] newRingHosts = new RingHost[l];
            System.arraycopy(ringHosts, 1, newRingHosts, 0, l);
            return solve(newRingHosts, partitionName, family, partitionCall, neighborQuorum, merger);
        } else if (consistency == Consistency.quorum) {
            randomizeArray(ringHosts);
            int neighborQuorum = consistency.quorum(ringHosts.length - 1);
            return solve(ringHosts, partitionName, family, partitionCall, 1 + neighborQuorum, merger);
        } else if (consistency == Consistency.write_one_read_all) {
            return solve(ringHosts, partitionName, family, partitionCall, ringHosts.length, merger);
        } else {
            throw new IllegalStateException("Unsupported write consistency:" + consistency.name());
        }
    }



    private <R, A> R solve(RingHost[] ringHosts, PartitionName partitionName, String family, PartitionCall<HttpClient, A, HttpClientException> partitionCall,
        int mandatory, Merger<R, A> merger) throws InterruptedException, Exception {
        try {
            Iterable<Callable<RingHostAnswer<A>>> callOrder = Iterables.transform(Arrays.asList(ringHosts),
                (ringHost) -> () -> {
                    return new RingHostAnswer<>(ringHost, clientProvider.call(partitionName, ringHost, family, partitionCall));
                });
            List<RingHostAnswer<A>> solutions = solve(callerThreads, callOrder.iterator(), mandatory, 1000); // TODO expose config
            return merger.merge(solutions);
        } catch (NotSolveableException nse) {
            partitionRoutingCache.invalidate(partitionName);
            throw nse;
        }
    }

    private <A> List<RingHostAnswer<A>> solve(Executor executor,
        Iterator<Callable<RingHostAnswer<A>>> solvers,
        int desireNumberOfResults,
        long addAdditionalSoverAfterNMillis) throws InterruptedException {

        CompletionService<RingHostAnswer<A>> completionService = new ExecutorCompletionService<>(executor);
        List<Future<RingHostAnswer<A>>> futures = new ArrayList<>();
        List<RingHostAnswer<A>> answers = new ArrayList<>();
        try {
            int pending = 0;
            for (int i = 0; i < desireNumberOfResults; i++) {
                if (solvers.hasNext()) {
                    pending++;
                    futures.add(completionService.submit(solvers.next()));
                }
            }
            while (answers.size() < desireNumberOfResults || !solvers.hasNext() && pending == 0) {
                Future<RingHostAnswer<A>> future = completionService.poll(addAdditionalSoverAfterNMillis, TimeUnit.MILLISECONDS);
                if (future == null) {
                    if (solvers.hasNext()) {
                        pending++;
                        futures.add(completionService.submit(solvers.next()));
                    }
                } else {
                    try {
                        RingHostAnswer<A> r = completionService.take().get();
                        if (r != null) {
                            answers.add(r);
                        }
                    } catch (ExecutionException ignore) {
                    } finally {
                        pending--;
                    }
                }
            }
            if (answers.size() != desireNumberOfResults) {
                throw new NotSolveableException("Not currently solveable. desire:" + desireNumberOfResults + " achieved:" + answers.size());
            }
        } finally {
            for (Future<RingHostAnswer<A>> f : futures) {
                f.cancel(true);
            }
        }
        return answers;
    }

    public static class NotSolveableException extends RuntimeException {

        public NotSolveableException(String message) {
            super(message);
        }

    }

    private static final Random RANDOM = new Random();  // Random number generator

    private static void randomizeArray(RingHost[] array) {

        for (int i = 0; i < array.length; i++) {
            int randomPosition = RANDOM.nextInt(array.length);
            RingHost temp = array[i];
            array[i] = array[randomPosition];
            array[randomPosition] = temp;
        }
    }

}
