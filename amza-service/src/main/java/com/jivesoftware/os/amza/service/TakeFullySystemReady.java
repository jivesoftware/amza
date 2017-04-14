package com.jivesoftware.os.amza.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.shared.BoundedExecutor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class TakeFullySystemReady implements AmzaSystemReady {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final SystemRingSizeProvider systemRingSizeProvider;
    private final PartitionCreator partitionCreator;
    private final SickPartitions sickPartitions;

    private final AtomicBoolean tookFully = new AtomicBoolean();
    private final AtomicBoolean ready = new AtomicBoolean();
    private final Map<VersionedPartitionName, Set<RingMember>> systemTookFully = Maps.newConcurrentMap();
    private final List<Callable<Void>> onReadyCallbacks = Collections.synchronizedList(Lists.newArrayList());

    public TakeFullySystemReady(SystemRingSizeProvider systemRingSizeProvider,
        PartitionCreator partitionCreator,
        SickPartitions sickPartitions,
        SickThreads sickThreads) {

        this.systemRingSizeProvider = systemRingSizeProvider;
        this.partitionCreator = partitionCreator;
        this.sickPartitions = sickPartitions;

        for (VersionedPartitionName versionedPartitionName : partitionCreator.getSystemPartitions()) {
            PartitionProperties properties = partitionCreator.getProperties(versionedPartitionName.getPartitionName());
            if (properties.replicated) {
                sickPartitions.sick(versionedPartitionName, new Throwable("System partition has not yet taken fully from a quorum"));
                systemTookFully.put(versionedPartitionName, Collections.newSetFromMap(Maps.newConcurrentMap()));
            }
        }

        ExecutorService readyExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("ready-%d").build());
        readyExecutor.submit(() -> {
            try {
                while (!tookFully.get()) {
                    Thread.sleep(100L);
                }

                ready();
                readyExecutor.shutdown();
            } catch (Exception e) {
                LOG.error("Encountered a problem while awaiting system ready, service will be parked");
                sickThreads.sick(e);
            }
        });
    }

    public boolean isReady() {
        return ready.get() & tookFully.get();
    }

    public void tookFully(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (ready.get() || !versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }

        Set<RingMember> ringMembers = systemTookFully.get(versionedPartitionName);
        if (ringMembers != null) {
            ringMembers.add(ringMember);
            checkReady();
        }
    }

    public void checkReady() throws Exception {
        int systemRingSize = systemRingSizeProvider.get();
        if (systemRingSize == -1) {
            LOG.error("System ready check could not determine system ring size, this will likely prevent system ready");
            return;
        } else if (systemRingSize == 0) {
            LOG.info("System ready check has not determined system ring size yet");
            return;
        }

        int quorum = systemRingSize / 2;
        boolean ready = true;
        for (Entry<VersionedPartitionName, Set<RingMember>> entry : systemTookFully.entrySet()) {
            VersionedPartitionName versionedPartitionName = entry.getKey();
            Set<RingMember> ringMembers = entry.getValue();
            if (ringMembers.size() < quorum || ringMembers.size() > systemRingSize) {
                ready = false;
                sickPartitions.sick(versionedPartitionName,
                    new Throwable("System partition has not yet taken fully from a quorum: " + ringMembers.size() + " / " + quorum));
            }
        }
        if (ready) {
            tookFully.set(true);
        }
    }

    private void ready() {
        synchronized (ready) {
            if (ready.compareAndSet(false, true)) {
                LOG.info("System is ready!");
                ready.set(true);

                ready.notifyAll();
                for (VersionedPartitionName versionedPartitionName : partitionCreator.getSystemPartitions()) {
                    sickPartitions.recovered(versionedPartitionName);
                }
                systemTookFully.clear();

                ExecutorService onReadyExecutor =  BoundedExecutor.newBoundedExecutor(1024, "on-ready-callback");
                try {
                    List<Future<?>> futures = Lists.newArrayList();
                    for (Callable<Void> callback : onReadyCallbacks) {
                        futures.add(onReadyExecutor.submit(callback));
                    }
                    for (Future<?> future : futures) {
                        future.get();
                    }
                    onReadyCallbacks.clear();
                } catch (Exception e) {
                    LOG.error("Failed onReady callback", e);
                    ready.set(false);
                } finally {
                    onReadyExecutor.shutdownNow();
                }
            }
        }
    }

    @Override
    public void await(long timeoutInMillis) throws Exception {
        if (ready.get()) {
            return;
        }
        checkReady();
        if (timeoutInMillis == 0) {
            throw new FailedToAchieveQuorumException("System has not reached ready state");
        }
        long end = System.currentTimeMillis() + timeoutInMillis;
        synchronized (ready) {
            while (!ready.get()) {
                long timeToWait = end - System.currentTimeMillis();
                if (timeToWait > 0) {
                    ready.wait(timeToWait);
                } else {
                    throw new FailedToAchieveQuorumException("Timed out waiting for system to reach ready state");
                }
            }
        }
    }

    public void onReady(Callable<Void> callable) throws Exception {
        synchronized (ready) {
            if (ready.get()) {
                callable.call();
            } else {
                onReadyCallbacks.add(callable);
            }
        }
    }

    public interface SystemRingSizeProvider {
        int get();
    }
}
