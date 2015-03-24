/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.service;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReceiver;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeReplicator;
import com.jivesoftware.os.amza.service.replication.AmzaRegionChangeTaker;
import com.jivesoftware.os.amza.service.replication.AmzaRegionCompactor;
import com.jivesoftware.os.amza.service.replication.MemoryBackedHighWaterMarks;
import com.jivesoftware.os.amza.service.replication.SendFailureListener;
import com.jivesoftware.os.amza.service.replication.TakeFailureListener;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.service.storage.WALs;
import com.jivesoftware.os.amza.shared.Marshaller;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALReplicator;
import com.jivesoftware.os.amza.shared.WALStorageProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

public class AmzaServiceInitializer {

    public static class AmzaServiceConfig {

        public String workingDirectory = "./var/data/";
        public int replicationFactor = 1;
        public int takeFromFactor = 1;
        public int resendReplicasIntervalInMillis = 1000;
        public int applyReplicasIntervalInMillis = 1000;
        public int takeFromNeighborsIntervalInMillis = 1000;
        public long checkIfCompactionIsNeededIntervalInMillis = 60_000;
        public long compactTombstoneIfOlderThanNMillis = 30 * 24 * 60 * 60 * 1000L;
    }

    public AmzaService initialize(AmzaServiceConfig config,
        AmzaStats amzaStats,
        RingHost ringHost,
        TimestampedOrderIdProvider orderIdProvider,
        Marshaller marshaller,
        WALStorageProvider regionsWALStorageProvider,
        WALStorageProvider replicaWALStorageProvider,
        WALStorageProvider resendWALStorageProvider,
        UpdatesSender updatesSender,
        UpdatesTaker updatesTaker,
        MemoryBackedHighWaterMarks highWaterMarks,
        Optional<SendFailureListener> sendFailureListener,
        Optional<TakeFailureListener> takeFailureListener,
        final RowChanges allRowChanges) throws Exception {

        RowChanges rowChanges = new RowChanges() {
            @Override
            public void changes(RowsChanged rowsChanged) throws Exception {
                allRowChanges.changes(rowsChanged);
            }
        };
        AmzaRegionWatcher amzaRegionWatcher = new AmzaRegionWatcher(rowChanges);

        File workingDirectory = new File(config.workingDirectory);

        final AtomicReference<AmzaRegionChangeReplicator> replicator = new AtomicReference<>();
        RegionProvider regionProvider = new RegionProvider(workingDirectory, "amza/stores", regionsWALStorageProvider, amzaRegionWatcher, new WALReplicator() {

            @Override
            public void replicate(RowsChanged rowsChanged) throws Exception {
                replicator.get().replicate(rowsChanged);
            }
        });
        final AmzaHostRing amzaRing = new AmzaHostRing(ringHost, regionProvider, orderIdProvider, marshaller);
        WALs resendWALs = new WALs(workingDirectory, "amza/WAL/resend", resendWALStorageProvider);

        AmzaRegionChangeReplicator changeReplicator = new AmzaRegionChangeReplicator(amzaStats,
            amzaRing,
            regionProvider,
            config.replicationFactor,
            resendWALs,
            updatesSender,
            sendFailureListener,
            config.resendReplicasIntervalInMillis);
        replicator.set(changeReplicator);

        WALs replicatedWALs = new WALs(workingDirectory, "amza/WAL/replicated", replicaWALStorageProvider);
        AmzaRegionChangeReceiver changeReceiver = new AmzaRegionChangeReceiver(amzaStats,
            regionProvider,
            replicatedWALs,
            config.applyReplicasIntervalInMillis);

        AmzaRegionChangeTaker changeTaker = new AmzaRegionChangeTaker(amzaStats,
            amzaRing,
            regionProvider,
            config.takeFromFactor,
            highWaterMarks,
            updatesTaker,
            takeFailureListener,
            config.takeFromNeighborsIntervalInMillis);

        AmzaRegionCompactor regionCompactor = new AmzaRegionCompactor(amzaStats,
            regionProvider,
            orderIdProvider,
            config.checkIfCompactionIsNeededIntervalInMillis,
            config.compactTombstoneIfOlderThanNMillis);

        AmzaService service = new AmzaService(orderIdProvider,
            marshaller,
            amzaRing,
            changeReceiver,
            changeTaker,
            changeReplicator,
            regionCompactor,
            regionProvider,
            amzaRegionWatcher);
        return service;
    }
}
