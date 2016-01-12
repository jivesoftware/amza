package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.scan.RowsChanged;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.wal.WALUpdated;
import com.jivesoftware.os.amza.service.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.StateStorage;

/**
 * @author jonathan.colt
 */
class AmzaStateStorage implements StateStorage<Long> {

    private final SystemWALStorage systemWALStorage;
    private final WALUpdated walUpdated;
    private final Member member;
    private final PartitionName partitionName;
    private final byte context;

    public AmzaStateStorage(SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        Member member,
        PartitionName partitionName,
        byte context) {
        this.systemWALStorage = systemWALStorage;
        this.walUpdated = walUpdated;
        this.member = member;
        this.partitionName = partitionName;
        this.context = context;
    }

    @Override
    public boolean scan(Member rootMember, Member otherMember, Long lifecycle, StateStream<Long> stream) throws Exception {
        byte[] fromKey = AmzaAquariumProvider.stateKey(partitionName, context, rootMember, lifecycle, otherMember);
        return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_STATE_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
            (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    return AmzaAquariumProvider.streamStateKey(key,
                        (partitionName, context, rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
                            State state = State.fromSerializedForm(value[0]);
                            return stream.stream(rootRingMember, isSelf, ackRingMember, partitionVersion, state, valueTimestamp, valueVersion);
                        });
                }
                return true;
            });
    }

    @Override
    public boolean update(StateUpdates<Long> updates) throws Exception {
        AmzaPartitionUpdates amzaPartitionUpdates = new AmzaPartitionUpdates();
        boolean result = updates.updates(
            (rootMember, otherMember, lifecycle, state, timestamp) -> {
                byte[] keyBytes = AmzaAquariumProvider.stateKey(partitionName, context, rootMember, lifecycle, otherMember);
                byte[] valueBytes = { state.getSerializedForm() };
                /*
                LOG.info("Context {} me:{} root:{} other:{} lifecycle:{} state:{} timestamp:{} on {}", context, member, rootMember, otherMember, lifecycle,
                    state, timestamp, partitionName);
                */
                amzaPartitionUpdates.set(keyBytes, valueBytes, timestamp);
                return true;
            });
        if (result && amzaPartitionUpdates.size() > 0) {
            RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, amzaPartitionUpdates, walUpdated);
            return !rowsChanged.isEmpty();
        } else {
            return false;
        }
    }

}
