package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.AmzaPartitionCommitable;
import com.jivesoftware.os.amza.service.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.aquarium.interfaces.StateStorage.StateUpdates;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by jonathan.colt on 4/19/17.
 */
public class AmzaStateStorageFlusher {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final SystemWALStorage systemWALStorage;
    private final OrderIdProvider orderIdProvider;
    private final WALUpdated walUpdated;

    private final Semaphore semaphore = new Semaphore(Short.MAX_VALUE);
    private final AtomicReference<AmzaPartitionUpdates> partitionUpdates = new AtomicReference<>(new AmzaPartitionUpdates());
    private final AtomicLong flushVersion = new AtomicLong();

    public AmzaStateStorageFlusher(SystemWALStorage systemWALStorage,
        OrderIdProvider orderIdProvider,
        WALUpdated walUpdated) {

        this.systemWALStorage = systemWALStorage;
        this.orderIdProvider = orderIdProvider;
        this.walUpdated = walUpdated;

        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                try {
                    long currentVersion = flushVersion.get();
                    flush();
                    synchronized (flushVersion) {
                        if (currentVersion == flushVersion.get()) {
                            flushVersion.wait();
                        }
                    }
                } catch (Throwable t) {
                    LOG.error("AmzaStateStorage failure", t);
                }
            }
        });
    }


    public boolean update(PartitionName partitionName,
        byte context,
        StateUpdates<Long> updates) throws Exception {

        semaphore.acquire();
        try {
            AmzaPartitionUpdates amzaPartitionUpdates = partitionUpdates.get();
            updates.updates(
                (rootMember, otherMember, lifecycle, state, timestamp) -> {
                    byte[] keyBytes = AmzaAquariumProvider.stateKey(partitionName, context, rootMember, lifecycle, otherMember);
                    byte[] valueBytes = { state.getSerializedForm() };
                    amzaPartitionUpdates.set(keyBytes, valueBytes, timestamp);
                    return true;
                });
            flushVersion.incrementAndGet();
            synchronized (flushVersion) {
                flushVersion.notifyAll();
            }
            return false;
        } finally {
            semaphore.release();
        }
    }

    public boolean flush() throws Exception {
        semaphore.acquire(Short.MAX_VALUE);
        try {
            AmzaPartitionUpdates amzaPartitionUpdates = partitionUpdates.get();
            int size = amzaPartitionUpdates.size();
            if (size > 0) {

                LOG.inc("stateStoreFlushed>pow>" + UIO.chunkPower(size, 0));
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX,
                    null,
                    new AmzaPartitionCommitable(amzaPartitionUpdates, orderIdProvider),
                    walUpdated);
                partitionUpdates.set(new AmzaPartitionUpdates());
                return !rowsChanged.isEmpty();
            } else {
                return false;
            }
        } finally {
            semaphore.release(Short.MAX_VALUE);
        }
    }

}
