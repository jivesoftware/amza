package com.jivesoftware.os.amza.service.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.service.storage.RegionIndex;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorage;
import com.jivesoftware.os.amza.shared.WALStorageUpdateMode;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.amza.storage.RowMarshaller;
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
public class AmzaRegionChangeReceiver {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static RegionName RECEIVED = new RegionName(true, "received", "received");

    private ScheduledExecutorService applyThreadPool;
    private ScheduledExecutorService compactThreadPool;

    private final AmzaStats amzaStats;
    private final RowMarshaller<byte[]> rowMarshaller;
    private final RegionIndex regionIndex;
    private final RegionStripeProvider regionStripeProvider;
    private final WALs receivedWAL;
    private final long applyReplicasIntervalInMillis;
    private final int numberOfApplierThreads;
    private final Object[] receivedLocks;
    private final Map<RegionName, Long> highwaterMarks = new ConcurrentHashMap<>();

    public AmzaRegionChangeReceiver(AmzaStats amzaStats,
        RowMarshaller<byte[]> rowMarshaller,
        RegionIndex regionIndex,
        RegionStripeProvider regionStripeProvider,
        WALs receivedUpdatesWAL,
        long applyReplicasIntervalInMillis,
        int numberOfApplierThreads) {
        this.amzaStats = amzaStats;
        this.rowMarshaller = rowMarshaller;
        this.regionIndex = regionIndex;
        this.regionStripeProvider = regionStripeProvider;
        this.receivedWAL = receivedUpdatesWAL;
        this.applyReplicasIntervalInMillis = applyReplicasIntervalInMillis;
        this.numberOfApplierThreads = numberOfApplierThreads;
        this.receivedLocks = new Object[numberOfApplierThreads];
        for (int i = 0; i < receivedLocks.length; i++) {
            receivedLocks[i] = new Object();
        }
    }

    public void receiveChanges(RegionName regionName, final Scannable<WALValue> changes) throws Exception {

        final byte[] regionNameBytes = regionName.toBytes();
        final Scannable<WALValue> receivedScannable = new Scannable<WALValue>() {

            @Override
            public void rowScan(final Scan<WALValue> walScan) throws Exception {
                changes.rowScan(new Scan<WALValue>() {

                    @Override
                    public boolean row(long rowTxId, WALKey key, WALValue value) throws Exception {
                        ByteBuffer bb = ByteBuffer.allocate(2 + regionNameBytes.length + 4 + key.getKey().length);
                        bb.putShort((short) regionNameBytes.length);
                        bb.put(regionNameBytes);
                        bb.putInt(key.getKey().length);
                        bb.put(key.getKey());
                        return walScan.row(rowTxId, new WALKey(bb.array()), value);
                    }
                });
            }
        };

        RegionName receivedRegionName = regionIndex.exists(regionName) ? RECEIVED : regionName;
        RowsChanged changed = receivedWAL.execute(receivedRegionName, new WALs.Tx<RowsChanged>() {
            @Override
            public RowsChanged execute(WALStorage storage) throws Exception {
                return storage.update(null, null, WALStorageUpdateMode.noReplication, receivedScannable);
            }
        });

        amzaStats.received(regionName, changed.getApply().size(), changed.getOldestRowTxId());

        Object lock = receivedLocks[Math.abs(regionName.hashCode()) % numberOfApplierThreads];
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    synchronized public void start() {
        if (applyThreadPool == null) {
            applyThreadPool = Executors.newScheduledThreadPool(numberOfApplierThreads, new ThreadFactoryBuilder().setNameFormat("applyReceivedChanges-%d")
                .build());
            //for (int i = 0; i < numberOfApplierThreads; i++) {
            //    final int stripe = i;
            applyThreadPool.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        applyReceivedChanges(0);
                    } catch (Throwable x) {
                        LOG.warn("Shouldn't have gotten here. Implementors please catch your exceptions.", x);
                    }
                }
            }, applyReplicasIntervalInMillis, applyReplicasIntervalInMillis, TimeUnit.MILLISECONDS);
            //}
        }
        if (compactThreadPool == null) {
            compactThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("compactReceivedChanges-%d").build());
            compactThreadPool.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        compactReceivedChanges();
                    } catch (Throwable x) {
                        LOG.error("Failing to compact.", x);
                    }
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
                for (final RegionName regionName : receivedWAL.getAllRegions()) {
                    try {
                        receivedWAL.execute(regionName, new WALs.Tx<Void>() {
                            @Override
                            public Void execute(WALStorage received) throws Exception {
                                Long highWatermark = highwaterMarks.get(regionName);
                                HighwaterInterceptor highwaterInterceptor = new HighwaterInterceptor(highWatermark, received);
                                MultiRegionWALScanBatchinator batchinator = new MultiRegionWALScanBatchinator(amzaStats, rowMarshaller, regionStripeProvider);
                                highwaterInterceptor.rowScan(batchinator);
                                if (batchinator.flush()) {
                                    highwaterMarks.put(regionName, highwaterInterceptor.getHighwater());
                                    appliedChanges.setValue(true);
                                }
                                return null;
                            }
                        });
                    } catch (Exception x) {
                        LOG.warn("Apply receive changes failed for {}", new Object[]{regionName}, x);
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
        for (final RegionName regionName : receivedWAL.getAllRegions()) {
            final Long highWatermark = highwaterMarks.get(regionName);
            if (highWatermark != null) {
                boolean compactedToEmpty = receivedWAL.execute(regionName, new WALs.Tx<Boolean>() {
                    @Override
                    public Boolean execute(WALStorage regionWAL) throws Exception {
                        amzaStats.beginCompaction("Compacting Received:" + regionName);
                        try {
                            long sizeInBytes = regionWAL.compactTombstone(highWatermark, highWatermark);
                            return sizeInBytes == 0;
                        } finally {
                            amzaStats.endCompaction("Compacting Received:" + regionName);
                        }
                    }
                });
                if (compactedToEmpty && !regionName.equals(RECEIVED)) {
                    receivedWAL.removeIfEmpty(regionName);
                }
            }
        }
    }
}
