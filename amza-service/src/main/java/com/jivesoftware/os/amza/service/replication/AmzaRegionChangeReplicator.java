package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaRingReader;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RingMember;
import com.jivesoftware.os.amza.shared.RingNeighbors;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class AmzaRegionChangeReplicator implements WALReplicator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService resendThreadPool;
    private ScheduledExecutorService compactThreadPool;
    private final AmzaStats amzaStats;
    private final PrimaryRowMarshaller<byte[]> rowMarshaller;
    private final AmzaRingReader amzaRing;
    private final RegionIndex regionIndex;
    private final WALs resendWAL;
    private final UpdatesSender updatesSender;
    private final ExecutorService sendExecutor;
    private final Optional<SendFailureListener> sendFailureListener;
    private final Map<RegionName, Long> highwaterMarks = new ConcurrentHashMap<>();
    private final long resendReplicasIntervalInMillis;
    private final int numberOfResendThreads;

    public AmzaRegionChangeReplicator(AmzaStats amzaStats,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        AmzaRingReader amzaRing,
        RegionIndex regionIndex,
        WALs resendWAL,
        UpdatesSender updatesSender,
        ExecutorService sendExecutor,
        Optional<SendFailureListener> sendFailureListener,
        long resendReplicasIntervalInMillis,
        int numberOfResendThreads) {

        this.amzaStats = amzaStats;
        this.rowMarshaller = rowMarshaller;
        this.amzaRing = amzaRing;
        this.regionIndex = regionIndex;
        this.resendWAL = resendWAL;
        this.updatesSender = updatesSender;
        this.sendExecutor = sendExecutor;
        this.sendFailureListener = sendFailureListener;
        this.resendReplicasIntervalInMillis = resendReplicasIntervalInMillis;
        this.numberOfResendThreads = numberOfResendThreads;
    }

    synchronized public void start() throws Exception {

        if (resendThreadPool == null) {
            resendThreadPool = Executors.newScheduledThreadPool(numberOfResendThreads,
                new ThreadFactoryBuilder().setNameFormat("resendLocalChanges-%d").build());
            for (int i = 0; i < numberOfResendThreads; i++) {
                final int stripe = i;
                resendThreadPool.scheduleWithFixedDelay(() -> {
                    try {
                        resendLocalChanges(stripe);
                    } catch (Throwable x) {
                        LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
                    }
                }, resendReplicasIntervalInMillis, resendReplicasIntervalInMillis, TimeUnit.MILLISECONDS);
            }
        }

        if (compactThreadPool == null) {
            compactThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("compactResendChanges-%d").build());
            compactThreadPool.scheduleWithFixedDelay(() -> {
                try {
                    compactResendChanges();
                } catch (Throwable x) {
                    LOG.warn("Shouldn't have gotten here. Implements please catch your expections.", x);
                }
            }, 1, 1, TimeUnit.MINUTES);

        }
    }

    synchronized public void stop() throws Exception {
        if (resendThreadPool != null) {
            this.resendThreadPool.shutdownNow();
            this.resendThreadPool = null;
        }

        if (compactThreadPool != null) {
            this.compactThreadPool.shutdownNow();
            this.compactThreadPool = null;
        }
    }

    @Override
    public Future<Boolean> replicate(RowsChanged rowsChanged) throws Exception {
        return replicateLocalUpdates(rowsChanged.getRegionName(), rowsChanged, true);
    }

    public Future<Boolean> replicateLocalUpdates(
        final RegionName regionName,
        final Commitable<WALValue> updates,
        final boolean enqueueForResendOnFailure) throws Exception {

        final RegionProperties regionProperties = regionIndex.getProperties(regionName);
        return sendExecutor.submit(() -> {
            if (regionProperties != null) {
                if (!regionProperties.disabled && regionProperties.replicationFactor > 0) {
                    RingNeighbors ringNeighbors = amzaRing.getRingNeighbors(regionName.getRingName());
                    Entry<RingMember, RingHost>[] below = ringNeighbors.getBelowRing();
                    if (below == null || below.length == 0) {
                        if (enqueueForResendOnFailure) {
                            resendWAL.execute(regionName,
                                (WALStorage resend) -> {
                                    resend.update(false, null, WALStorageUpdateMode.noReplication, updates);
                                    return null;
                                });
                        }
                        return false;
                    } else {
                        int numReplicated = replicateUpdatesToRingHosts(regionName, updates, enqueueForResendOnFailure, below,
                            regionProperties.replicationFactor);
                        return numReplicated >= regionProperties.replicationFactor;
                    }
                } else {
                    return true;
                }
            } else {
                if (enqueueForResendOnFailure) {
                    resendWAL.execute(regionName,
                        (WALStorage resend) -> {
                            resend.update(false, null, WALStorageUpdateMode.noReplication, updates);
                            return null;
                        });
                }
                return false;
            }
        });
    }

    public int replicateUpdatesToRingHosts(RegionName regionName,
        final Commitable<WALValue> updates,
        boolean enqueueForResendOnFailure,
        Entry<RingMember, RingHost>[] ringNode,
        int replicationFactor) throws Exception {

        RingWalker ringWalker = new RingWalker(ringNode, replicationFactor);
        Entry<RingMember, RingHost> node;
        while ((node = ringWalker.node()) != null) {
            try {
                updatesSender.sendUpdates(node, regionName, updates);
                amzaStats.offered(node);
                amzaStats.replicateErrors.setCount(node.getKey(), 0);
                if (sendFailureListener.isPresent()) {
                    sendFailureListener.get().sent(node);
                }
                ringWalker.success();
            } catch (Exception x) {
                if (sendFailureListener.isPresent()) {
                    sendFailureListener.get().failedToSend(node, x);
                }
                ringWalker.failed();
                if (amzaStats.replicateErrors.count(node) == 0) {
                    LOG.warn("Can't replicate to host:{}", node);
                    LOG.trace("Can't replicate to host:{} region:{} takeFromFactor:{}", new Object[]{node, regionName, replicationFactor}, x);
                }
                amzaStats.replicateErrors.add(node.getKey());
                if (enqueueForResendOnFailure) {
                    resendWAL.execute(regionName, (WALStorage resend) -> {
                        resend.update(false, null, WALStorageUpdateMode.noReplication, updates);
                        return null;
                    });
                    enqueueForResendOnFailure = false;
                }
            }
        }
        return ringWalker.getNumReplicated();
    }

    void resendLocalChanges(int stripe) throws Exception {

        for (final RegionName regionName : regionIndex.getActiveRegions()) {
            if (Math.abs(regionName.hashCode()) % numberOfResendThreads == stripe) {
                RingNeighbors hostRing = amzaRing.getRingNeighbors(regionName.getRingName());
                Entry<RingMember, RingHost>[] belowRing = hostRing.getBelowRing();
                if (belowRing.length > 0) {
                    resendWAL.execute(regionName, (WALStorage resend) -> {
                        Long highWatermark = highwaterMarks.get(regionName);
                        HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor(highWatermark, resend);
                        ReplicateBatchinator batchinator = new ReplicateBatchinator(rowMarshaller, regionName, AmzaRegionChangeReplicator.this);
                        highwaterInterceptor.rowScan(batchinator);
                        if (batchinator.flush()) {
                            highwaterMarks.put(regionName, highwaterInterceptor.getHighwater());
                        }
                        return null;
                    });
                } else {
                    LOG.warn("Trying to resend to an empty ring. regionName:" + regionName);
                }
            }
        }
    }

    void compactResendChanges() throws Exception {
        for (final RegionName regionName : regionIndex.getActiveRegions()) {
            final Long highWatermark = highwaterMarks.get(regionName);
            if (highWatermark != null) {
                boolean compactedToEmpty = resendWAL.execute(regionName, (WALStorage regionWAL) -> {
                    amzaStats.beginCompaction("Compacting Resend:" + regionName);
                    try {
                        long sizeInBytes = regionWAL.compactTombstone(highWatermark, highWatermark);
                        return sizeInBytes == 0;
                    } catch (Exception x) {
                        LOG.warn("Failing to compact:" + regionName, x);
                        return false;
                    } finally {
                        amzaStats.endCompaction("Compacting Resend:" + regionName);
                    }
                });
                if (compactedToEmpty) {
                    resendWAL.removeIfEmpty(regionName);
                }
            }
        }
    }
}
