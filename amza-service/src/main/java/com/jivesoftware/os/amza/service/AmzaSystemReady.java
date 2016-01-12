package com.jivesoftware.os.amza.service;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.ring.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.PartitionIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class AmzaSystemReady {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaRingStoreReader ringStoreReader;
    private final PartitionIndex partitionIndex;
    private final SickPartitions sickPartitions;

    private final AtomicBoolean ready = new AtomicBoolean();
    private final Map<VersionedPartitionName, Set<RingMember>> systemTookFully = Maps.newConcurrentMap();

    public AmzaSystemReady(AmzaRingStoreReader ringStoreReader, PartitionIndex partitionIndex, SickPartitions sickPartitions) {
        this.ringStoreReader = ringStoreReader;
        this.partitionIndex = partitionIndex;
        this.sickPartitions = sickPartitions;

        for (VersionedPartitionName versionedPartitionName : partitionIndex.getSystemPartitions()) {
            PartitionProperties properties = partitionIndex.getProperties(versionedPartitionName.getPartitionName());
            if (properties.takeFromFactor > 0) {
                sickPartitions.sick(versionedPartitionName, new Throwable("System partition has not yet taken fully from a quorum"));
                systemTookFully.put(versionedPartitionName, Collections.newSetFromMap(Maps.newConcurrentMap()));
            }
        }
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

    private void checkReady() throws Exception {
        int systemRingSize = ringStoreReader.getRingSize(AmzaRingReader.SYSTEM_RING);
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
            ready();
        }
    }

    private void ready() {
        synchronized (ready) {
            LOG.info("System is ready!");
            ready.set(true);
            ready.notifyAll();
            for (VersionedPartitionName versionedPartitionName : partitionIndex.getSystemPartitions()) {
                sickPartitions.recovered(versionedPartitionName);
            }
            systemTookFully.clear();
        }
    }

    public void await(long timeoutInMillis) throws Exception {
        if (!ready.get()) {
            if (timeoutInMillis == 0) {
                throw new FailedToAchieveQuorumException("System has not reached ready state");
            }
            synchronized (ready) {
                while (!ready.get()) {
                    if (timeoutInMillis == 0) {
                        throw new FailedToAchieveQuorumException("Timed out waiting for system to reach ready state");
                    }
                    long start = System.currentTimeMillis();
                    ready.wait(timeoutInMillis);
                    timeoutInMillis = Math.max(timeoutInMillis - (start - System.currentTimeMillis()), 0);
                }
            }
        }
    }
}
