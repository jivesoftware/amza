package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.Commitable;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.TxRegionStatus;
import com.jivesoftware.os.amza.shared.VersionedRegionName;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.PrimaryRowMarshaller;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableBoolean;

/**
 * @author jonathan.colt
 */
public class RegionChangeReceiver {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final VersionedRegionName RECEIVED = new VersionedRegionName(new RegionName(true, "received", "received"), 0);

    private ScheduledExecutorService applyThreadPool;
    private ScheduledExecutorService compactThreadPool;

    private final AmzaStats amzaStats;
    private final PrimaryRowMarshaller<byte[]> rowMarshaller;
    private final TxRegionStatus txRegionState;
    private final RegionStripeProvider regionStripeProvider;
    private final WALs receivedWAL;
    private final long applyReplicasIntervalInMillis;
    private final int numberOfApplierThreads;
    private final Object[] receivedLocks;
    private final Map<VersionedRegionName, Long> highwaterMarks = new ConcurrentHashMap<>();

    public RegionChangeReceiver(AmzaStats amzaStats,
        PrimaryRowMarshaller<byte[]> rowMarshaller,
        TxRegionStatus txRegionState,
        RegionStripeProvider regionStripeProvider,
        WALs receivedUpdatesWAL,
        long applyReplicasIntervalInMillis,
        int numberOfApplierThreads) {
        this.amzaStats = amzaStats;
        this.rowMarshaller = rowMarshaller;
        this.txRegionState = txRegionState;
        this.regionStripeProvider = regionStripeProvider;
        this.receivedWAL = receivedUpdatesWAL;
        this.applyReplicasIntervalInMillis = applyReplicasIntervalInMillis;
        this.numberOfApplierThreads = numberOfApplierThreads;
        this.receivedLocks = new Object[numberOfApplierThreads];
        for (int i = 0; i < receivedLocks.length; i++) {
            receivedLocks[i] = new Object();
        }
    }

    public void receiveChanges(RegionName regionName, final Commitable<WALValue> changes) throws Exception {

        txRegionState.tx(regionName, (versionedRegionName, regionStatus) -> {

            if (regionStatus == null || regionStatus == TxRegionStatus.Status.DISPOSE) {
                throw new IllegalStateException("regionName:" + regionName + " doesn't exist.");
            }

            byte[] versionedRegionNameBytes = versionedRegionName.toBytes();
            Commitable<WALValue> receivedScannable = (highwaters, walScan) -> {
                changes.commitable(highwaters, (long rowTxId, WALKey key, WALValue value) -> {
                    ByteBuffer bb = ByteBuffer.allocate(2 + versionedRegionNameBytes.length + 4 + key.getKey().length);
                    bb.putShort((short) versionedRegionNameBytes.length);
                    bb.put(versionedRegionNameBytes);
                    bb.putInt(key.getKey().length);
                    bb.put(key.getKey());
                    return walScan.row(rowTxId, new WALKey(bb.array()), value);
                });
            };

            VersionedRegionName sendToRegion = regionStatus == TxRegionStatus.Status.ONLINE ? RECEIVED : versionedRegionName;

            RowsChanged changed = receivedWAL.execute(sendToRegion, (WALStorage storage) -> {
                return storage.update(false, null, WALStorageUpdateMode.noReplication, receivedScannable);
            });

            amzaStats.received(regionName, changed.getApply().size(), changed.getOldestRowTxId());
            Object lock = receivedLocks[Math.abs(regionName.hashCode()) % numberOfApplierThreads];
            synchronized (lock) {
                lock.notifyAll();
            }
            return null;
        });

    }

    synchronized public void start() {
        if (applyThreadPool == null) {
            applyThreadPool = Executors.newScheduledThreadPool(numberOfApplierThreads, new ThreadFactoryBuilder().setNameFormat("applyReceivedChanges-%d")
                .build());
            //for (int i = 0; i < numberOfApplierThreads; i++) {
            //    final int stripe = i;
            applyThreadPool.scheduleWithFixedDelay(() -> {
                try {
                    applyReceivedChanges(0);
                } catch (Throwable x) {
                    LOG.warn("Shouldn't have gotten here. Implementors please catch your exceptions.", x);
                }
            }, applyReplicasIntervalInMillis, applyReplicasIntervalInMillis, TimeUnit.MILLISECONDS);
            //}
        }
        if (compactThreadPool == null) {
            compactThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("compactReceivedChanges-%d").build());
            compactThreadPool.scheduleWithFixedDelay(() -> {
                try {
                    compactReceivedChanges();
                } catch (Throwable x) {
                    LOG.error("Failing to compact.", x);
                }
            }, 1, 1, TimeUnit.MINUTES);
        }
    }

    synchronized public void stop() {
        if (applyThreadPool != null) {
            this.applyThreadPool.shutdownNow();
            this.applyThreadPool = null;
        }
        if (compactThreadPool != null) {
            this.compactThreadPool.shutdownNow();
            this.compactThreadPool = null;
        }
    }

    private void applyReceivedChanges(int stripe) throws Exception {

        while (true) {
            final MutableBoolean appliedChanges = new MutableBoolean(false);
            final MutableBoolean failedChanges = new MutableBoolean(false);
            try {
                for (VersionedRegionName versionedRegionName : receivedWAL.getAllRegions()) {
                    try {
                        receivedWAL.execute(versionedRegionName, (WALStorage received) -> {
                            Long highWatermark = highwaterMarks.get(versionedRegionName);
                            HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor(highWatermark, received);
                            MultiRegionWALScanBatchinator batchinator = new MultiRegionWALScanBatchinator(amzaStats, rowMarshaller, regionStripeProvider);
                            highwaterInterceptor.rowScan(batchinator);
                            if (batchinator.flush()) {
                                highwaterMarks.put(versionedRegionName, highwaterInterceptor.getHighwater());
                                appliedChanges.setValue(true);
                            }
                            return null;
                        });
                    } catch (Exception x) {
                        LOG.warn("Apply receive changes failed for {}", new Object[]{versionedRegionName}, x);
                        failedChanges.setValue(true);
                    }
                }

                if (!appliedChanges.booleanValue()) {
                    synchronized (receivedLocks[stripe]) {
                        receivedLocks[stripe].wait(failedChanges.booleanValue() ? 1_000 : 0);
                    }
                }
            } catch (Exception x) {
                LOG.error("Failed to apply received.", x);
            }
        }
    }

    private void compactReceivedChanges() throws Exception {
        for (VersionedRegionName versionedRegionName : receivedWAL.getAllRegions()) {
            final Long highWatermark = highwaterMarks.get(versionedRegionName);
            if (highWatermark != null) {
                boolean compactedToEmpty = receivedWAL.execute(versionedRegionName, (WALStorage regionWAL) -> {
                    amzaStats.beginCompaction("Compacting Received:" + versionedRegionName);
                    try {
                        long sizeInBytes = regionWAL.compactTombstone(highWatermark, highWatermark);
                        return sizeInBytes == 0;
                    } finally {
                        amzaStats.endCompaction("Compacting Received:" + versionedRegionName);
                    }
                });
                if (compactedToEmpty && !versionedRegionName.equals(RECEIVED)) {
                    receivedWAL.removeIfEmpty(versionedRegionName);
                }
            }
        }
    }
}
