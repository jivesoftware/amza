package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.interfaces.StateStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
class AmzaStateStorage implements StateStorage<Long> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AmzaInterner amzaInterner;
    private final SystemWALStorage systemWALStorage;
    private final AmzaStateStorageFlusher amzaStateStorageFlusher;
    private final PartitionName partitionName;
    private final byte context;

    public AmzaStateStorage(AmzaInterner amzaInterner,
        SystemWALStorage systemWALStorage,
        AmzaStateStorageFlusher amzaStateStorageFlusher,
        PartitionName partitionName,
        byte context) {
        this.amzaInterner = amzaInterner;
        this.systemWALStorage = systemWALStorage;
        this.amzaStateStorageFlusher = amzaStateStorageFlusher;
        this.partitionName = partitionName;
        this.context = context;
    }

    @Override
    public boolean scan(Member rootMember, Member otherMember, Long lifecycle, StateStream<Long> stream) throws Exception {
        byte[] fromKey = AmzaAquariumProvider.stateKey(partitionName, context, rootMember, lifecycle, otherMember);
        return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_STATE_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
            (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    return AmzaAquariumProvider.streamStateKey(key, amzaInterner,
                        (partitionName, context, rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
                            State state = State.fromSerializedForm(value[0]);
                            return stream.stream(rootRingMember, isSelf, ackRingMember, partitionVersion, state, valueTimestamp, valueVersion);
                        });
                }
                return true;
            }, true);
    }

    @Override
    public boolean update(StateUpdates<Long> updates) throws Exception {
        return amzaStateStorageFlusher.update(partitionName, context, updates);
    }

}
