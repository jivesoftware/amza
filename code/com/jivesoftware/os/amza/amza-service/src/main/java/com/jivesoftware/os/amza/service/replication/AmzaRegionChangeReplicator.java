package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.AmzaHostRing;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpateMode;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jonathan.colt
 */
public class AmzaRegionChangeReplicator implements WALReplicator {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private ScheduledExecutorService resendThreadPool;
    private ScheduledExecutorService compactThreadPool;
    private final AmzaStats amzaStats;
    private final AmzaHostRing amzaRing;
    private final RegionProvider regionProvider;
    private final WALs resendWAL;
    private final UpdatesSender updatesSender;
    private final Optional<SendFailureListener> sendFailureListener;
    private final Map<RegionName, Long> highwaterMarks = new ConcurrentHashMap<>();
    private final long resendReplicasIntervalInMillis;
    private final int numberOfResendThreads;

    public AmzaRegionChangeReplicator(AmzaStats amzaStats,
        AmzaHostRing amzaRing,
        RegionProvider regionProvider,
        WALs resendWAL,
        UpdatesSender updatesSender,
        Optional<SendFailureListener> sendFailureListener,
        long resendReplicasIntervalInMillis,
        int numberOfResendThreads) {

        this.amzaStats = amzaStats;
        this.amzaRing = amzaRing;
        this.regionProvider = regionProvider;
        this.resendWAL = resendWAL;
        this.updatesSender = updatesSender;
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
                resendThreadPool.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            resendLocalChanges(stripe);
                        } catch (Throwable x) {
                            LOG.warn("Failed while resending replicas.", x);
                        }
                    }
                }, resendReplicasIntervalInMillis, resendReplicasIntervalInMillis, TimeUnit.MILLISECONDS);
            }
        }

        if (compactThreadPool == null) {
            compactThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("compactResendChanges-%d").build());
            compactThreadPool.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        compactResendChanges();
                    } catch (Throwable x) {
                        LOG.error("Failing to compact.", x);
                    }
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
    public void replicate(RowsChanged rowsChanged) throws Exception {
        replicateLocalUpdates(rowsChanged.getRegionName(), rowsChanged, true);
    }

    public boolean replicateLocalUpdates(
        RegionName regionName,
        WALScanable updates,
        boolean enqueueForResendOnFailure) throws Exception {

        RegionProperties regionProperties = regionProvider.getRegionProperties(regionName);
        if (regionProperties != null) {
            if (!regionProperties.disabled && regionProperties.replicationFactor > 0) {
                HostRing hostRing = amzaRing.getHostRing(regionName.getRingName());
                RingHost[] ringHosts = hostRing.getBelowRing();
                if (ringHosts == null || ringHosts.length == 0) {
                    if (enqueueForResendOnFailure) {
                        WALStorage resend = resendWAL.get(regionName);
                        resend.update(WALStorageUpateMode.noReplication, updates);
                    }
                    return false;
                } else {
                    return replicateUpdatesToRingHosts(regionName, updates, enqueueForResendOnFailure, ringHosts, regionProperties.replicationFactor);
                }
            } else {
                return true;
            }
        } else {
            if (enqueueForResendOnFailure) {
                WALStorage resend = resendWAL.get(regionName);
                resend.update(WALStorageUpateMode.noReplication, updates);
            }
            return false;
        }
    }

    private boolean replicateUpdatesToRingHosts(RegionName regionName,
        WALScanable updates,
        boolean enqueueForResendOnFailure,
        RingHost[] ringHosts,
        int replicationFactor) throws Exception {

        RingWalker ringWalker = new RingWalker(ringHosts, replicationFactor);
        RingHost ringHost;
        while ((ringHost = ringWalker.host()) != null) {
            try {
                updatesSender.sendUpdates(ringHost, regionName, updates);
                amzaStats.offered(ringHost);
                if (sendFailureListener.isPresent()) {
                    sendFailureListener.get().sent(ringHost);
                }
                ringWalker.success();
            } catch (Exception x) {
                if (sendFailureListener.isPresent()) {
                    sendFailureListener.get().failedToSend(ringHost, x);
                }
                ringWalker.failed();
                LOG.info("Failed to send changeset to ringHost:{}", new Object[]{ringHost}, x);
                if (enqueueForResendOnFailure) {
                    WALStorage resend = resendWAL.get(regionName);
                    resend.update(WALStorageUpateMode.noReplication, updates);
                    enqueueForResendOnFailure = false;
                }
            }
        }
        return ringWalker.wasAdequatelyReplicated();
    }

    void resendLocalChanges(int stripe) throws Exception {

        for (RegionName regionName : regionProvider.getActiveRegions()) {
            if (Math.abs(regionName.hashCode()) % numberOfResendThreads == stripe) {
                HostRing hostRing = amzaRing.getHostRing(regionName.getRingName());
                RingHost[] ring = hostRing.getBelowRing();
                if (ring.length > 0) {
                    WALStorage resend = resendWAL.get(regionName);
                    Long highWatermark = highwaterMarks.get(regionName);
                    HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor("Replicate", highWatermark, resend);
                    ReplicateBatchinator batchinator = new ReplicateBatchinator(regionName, this);
                    highwaterInterceptor.rowScan(batchinator);
                    if (batchinator.flush()) {
                        highwaterMarks.put(regionName, highwaterInterceptor.getHighwater());
                    }
                } else {
                    LOG.warn("Trying to resend to an empty ring. regionName:" + regionName);
                }
            }
        }
    }

    void compactResendChanges() throws Exception {
        for (RegionName regionName : regionProvider.getActiveRegions()) {
            Long highWatermark = highwaterMarks.get(regionName);
            if (highWatermark != null) {
                WALStorage regionWAL = resendWAL.get(regionName);
                amzaStats.beginCompaction("Compacting Resend:" + regionName);
                try {
                    regionWAL.compactTombstone(highWatermark, highWatermark);
                } finally {
                    amzaStats.endCompaction("Compacting Resend:" + regionName);
                }
            }
        }
    }
}
