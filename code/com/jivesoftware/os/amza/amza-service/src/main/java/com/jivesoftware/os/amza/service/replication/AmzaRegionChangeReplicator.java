package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaHostRing;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.RegionStore;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALStorage;
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
    private ScheduledExecutorService scheduledThreadPool;
    private final AmzaStats amzaStats;
    private final AmzaHostRing amzaRing;
    private final RegionProvider regionProvider;
    private final int replicationFactor;
    private final WALs resendWAL;
    private final UpdatesSender updatesSender;
    private final Optional<SendFailureListener> sendFailureListener;
    private final Map<RegionName, Long> highwaterMarks = new ConcurrentHashMap<>();
    private final long resendReplicasIntervalInMillis;

    public AmzaRegionChangeReplicator(AmzaStats amzaStats,
        AmzaHostRing amzaRing,
        RegionProvider regionProvider,
        int replicationFactor,
        WALs resendWAL,
        UpdatesSender updatesSender,
        Optional<SendFailureListener> sendFailureListener,
        long resendReplicasIntervalInMillis) {

        this.amzaStats = amzaStats;
        this.amzaRing = amzaRing;
        this.regionProvider = regionProvider;
        this.replicationFactor = replicationFactor;
        this.resendWAL = resendWAL;
        this.updatesSender = updatesSender;
        this.sendFailureListener = sendFailureListener;
        this.resendReplicasIntervalInMillis = resendReplicasIntervalInMillis;
    }

    synchronized public void start() throws Exception {

        final int silenceBackToBackErrors = 100;
        if (scheduledThreadPool == null) {
            scheduledThreadPool = Executors.newScheduledThreadPool(2);
            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {
                int failedToSend = 0;

                @Override
                public void run() {
                    try {
                        failedToSend = 0;
                        resendLocalChanges();
                    } catch (Exception x) {
                        LOG.debug("Failed while resending replicas.", x);
                        if (failedToSend % silenceBackToBackErrors == 0) {
                            failedToSend++;
                            LOG.error("Failed while resending replicas.", x);
                        }
                    }
                }
            }, resendReplicasIntervalInMillis, resendReplicasIntervalInMillis, TimeUnit.MILLISECONDS);

            scheduledThreadPool.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        compactResendChanges();
                    } catch (Exception x) {
                        LOG.error("Failing to compact.", x);
                    }
                }
            }, 1, 1, TimeUnit.MINUTES);

        }
    }

    synchronized public void stop() throws Exception {
        if (scheduledThreadPool != null) {
            this.scheduledThreadPool.shutdownNow();
            this.scheduledThreadPool = null;
        }
    }

    @Override
    public void replicate(RowsChanged rowsChanged) throws Exception {
        if (replicateLocalUpdates(rowsChanged.getRegionName(), rowsChanged, true)) {
            amzaStats.replicated(null, rowsChanged); // TODO this is in the wrong place should be in replicateLocalUpdates
        }
    }

    private boolean replicateLocalUpdates(
        RegionName regionName,
        WALScanable updates,
        boolean enqueueForResendOnFailure) throws Exception {

        HostRing hostRing = amzaRing.getHostRing(regionName.getRingName());
        RingHost[] ringHosts = hostRing.getBelowRing();
        if (ringHosts == null || ringHosts.length == 0) {
            if (enqueueForResendOnFailure) {
                WALStorage resend = resendWAL.get(regionName);
                resend.update(updates);
            }
            return false;
        } else {
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
                        resend.update(updates);
                        enqueueForResendOnFailure = false;
                    }
                }
            }
            return ringWalker.wasAdequetlyReplicated();
        }
    }

    void resendLocalChanges() throws Exception {

        // TODO eval why this is Hacky. This loads resend tables for stored tables.
        for (Map.Entry<RegionName, RegionStore> region : regionProvider.getAll()) {
            resendWAL.get(region.getKey());
        }

        for (Map.Entry<RegionName, WALStorage> failedUpdates : resendWAL.getAll()) {
            RegionName regionName = failedUpdates.getKey();
            HostRing hostRing = amzaRing.getHostRing(regionName.getRingName());
            RingHost[] ring = hostRing.getBelowRing();
            if (ring.length > 0) {
                WALStorage resend = failedUpdates.getValue();
                Long highWatermark = highwaterMarks.get(regionName);
                HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor("Replicate", highWatermark, resend);
                if (replicateLocalUpdates(regionName, highwaterInterceptor, false)) {
                    highwaterMarks.put(regionName, highwaterInterceptor.getHighwater());
                }
            } else {
                LOG.warn("Trying to resend to an empty ring. regionName:" + regionName);
            }
        }
    }

    void compactResendChanges() throws Exception {
        for (Map.Entry<RegionName, WALStorage> updates : resendWAL.getAll()) {
            RegionName regionName = updates.getKey();
            WALStorage regionWAL = updates.getValue();
            Long highWatermark = highwaterMarks.get(regionName);
            if (highWatermark != null) {
                amzaStats.beginCompaction("Compacting Resend:" + regionName);
                try {
                    regionWAL.compactTombstone(highWatermark); // TODO should this be plus 1
                } finally {
                    amzaStats.endCompaction("Compacting Resend:" + regionName);
                }
            }
        }
    }
}
