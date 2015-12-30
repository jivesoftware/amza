package com.jivesoftware.os.amza.service.replication;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.StateStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * @author jonathan.colt
 */
class AmzaStateStorage implements StateStorage<Long> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final SystemWALStorage systemWALStorage;
    private final WALUpdated walUpdated;
    private final Member member;
    private final VersionedPartitionName versionedPartitionName;
    private final byte context;
    private final long startupVersion;

    public AmzaStateStorage(SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        Member member,
        VersionedPartitionName versionedPartitionName,
        byte context,
        long startupVersion) {
        this.systemWALStorage = systemWALStorage;
        this.walUpdated = walUpdated;
        this.member = member;
        this.versionedPartitionName = versionedPartitionName;
        this.context = context;
        this.startupVersion = startupVersion;
    }

    @Override
    public boolean scan(Member rootMember, Member otherMember, Long lifecycle, StateStream<Long> stream) throws Exception {
        byte[] fromKey = AmzaAquariumProvider.stateKey(versionedPartitionName.getPartitionName(), context, rootMember, lifecycle, otherMember);
        return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_STATE_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
            (rowType, prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    return AmzaAquariumProvider.streamStateKey(key,
                        (partitionName, context, rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
                            if (!rootRingMember.equals(member) || valueVersion > startupVersion) {
                                State state = State.fromSerializedForm(value[0]);
                                return stream.stream(rootRingMember, isSelf, ackRingMember, partitionVersion, state, valueTimestamp, valueVersion);
                            } else {
                                return true;
                            }
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
                byte[] keyBytes = AmzaAquariumProvider.stateKey(versionedPartitionName.getPartitionName(), context, rootMember, lifecycle, otherMember);
                byte[] valueBytes = { state.getSerializedForm() };
                /*
                LOG.info("Context {} me:{} root:{} other:{} lifecycle:{} state:{} timestamp:{} on {}", context, member, rootMember, otherMember, lifecycle,
                    state, timestamp, versionedPartitionName);
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
