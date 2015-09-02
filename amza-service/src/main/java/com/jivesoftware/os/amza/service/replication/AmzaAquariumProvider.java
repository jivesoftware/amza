package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.aquarium.Aquarium;
import com.jivesoftware.os.amza.aquarium.Liveliness;
import com.jivesoftware.os.amza.aquarium.LivelinessStorage;
import com.jivesoftware.os.amza.aquarium.Member;
import com.jivesoftware.os.amza.aquarium.MemberLifecycle;
import com.jivesoftware.os.amza.aquarium.ReadWaterline;
import com.jivesoftware.os.amza.aquarium.RingSize;
import com.jivesoftware.os.amza.aquarium.State;
import com.jivesoftware.os.amza.aquarium.StateStorage;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class AmzaAquariumProvider {

    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;

    private final OrderIdProvider orderIdProvider;
    private final AmzaRingReader amzaRingReader;
    private final SystemWALStorage systemWALStorage;
    private final StorageVersionProvider storageVersionProvider;
    private final WALUpdated walUpdated;
    private final long deadAfterMillis;
    private final AtomicLong firstLivelinessTimestamp = new AtomicLong(-1);

    public AmzaAquariumProvider(OrderIdProvider orderIdProvider,
        AmzaRingReader amzaRingReader,
        SystemWALStorage systemWALStorage,
        StorageVersionProvider storageVersionProvider,
        WALUpdated walUpdated,
        long deadAfterMillis) {
        this.orderIdProvider = orderIdProvider;
        this.amzaRingReader = amzaRingReader;
        this.systemWALStorage = systemWALStorage;
        this.storageVersionProvider = storageVersionProvider;
        this.walUpdated = walUpdated;
        this.deadAfterMillis = deadAfterMillis;
    }

    public void tookFully(RingMember rootRingMember, RingMember fromRingMember, VersionedPartitionName versionedPartitionName) throws Exception {

    }

    public Aquarium getAquarium(RingMember rootRingMember, VersionedPartitionName versionedPartitionName) throws Exception {
        Member rootMember = rootRingMember.asAquariumMember();
        AmzaLivelinessStorage livelinessStorage = new AmzaLivelinessStorage(systemWALStorage, walUpdated, rootMember);
        ReadWaterline readCurrent = new ReadWaterline<>(
            new AmzaStateStorage(systemWALStorage, storageVersionProvider, amzaRingReader, walUpdated, rootMember, versionedPartitionName, CURRENT),
            livelinessStorage,
            new AmzaMemberLifecycle(storageVersionProvider, versionedPartitionName),
            new AmzaRingSize(amzaRingReader, versionedPartitionName),
            rootRingMember.asAquariumMember(),
            deadAfterMillis,
            Long.class);
        ReadWaterline readDesired = new ReadWaterline<>(
            new AmzaStateStorage(systemWALStorage, storageVersionProvider, amzaRingReader, walUpdated, rootMember, versionedPartitionName, DESIRED),
            livelinessStorage,
            new AmzaMemberLifecycle(storageVersionProvider, versionedPartitionName),
            new AmzaRingSize(amzaRingReader, versionedPartitionName),
            rootRingMember.asAquariumMember(),
            deadAfterMillis,
            Long.class);

        return new Aquarium(orderIdProvider,
            (member, tx) -> tx.tx(readCurrent, readDesired),
            new Liveliness(livelinessStorage, rootMember, firstLivelinessTimestamp),
            (current, desiredTimestamp, state) -> {
                //TODO make sure this is a valid transition
                byte[] keyBytes = stateKey(versionedPartitionName.getPartitionName(), CURRENT, versionedPartitionName.getPartitionVersion(),
                    rootMember, rootMember);
                byte[] valueBytes = { state.getSerializedForm() };
                AmzaPartitionUpdates updates = new AmzaPartitionUpdates().set(keyBytes, valueBytes, desiredTimestamp);
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, updates, walUpdated);
                return !rowsChanged.isEmpty();
            },
            (current, desiredTimestamp, state) -> {
                //TODO make sure this is a valid transition
                byte[] keyBytes = stateKey(versionedPartitionName.getPartitionName(), DESIRED, versionedPartitionName.getPartitionVersion(),
                    rootMember, rootMember);
                byte[] valueBytes = { state.getSerializedForm() };
                AmzaPartitionUpdates updates = new AmzaPartitionUpdates().set(keyBytes, valueBytes, desiredTimestamp);
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, updates, walUpdated);
                return !rowsChanged.isEmpty();
            },
            rootRingMember.asAquariumMember());
    }

    private static byte[] stateKey(PartitionName partitionName,
        byte context,
        long partitionVersion,
        Member rootRingMember,
        Member ackRingMember) throws Exception {

        int partitionSizeInBytes = 4 + partitionName.sizeInBytes();
        if (rootRingMember != null && ackRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            int ackSizeInBytes = 4 + ackRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + 8 + rootSizeInBytes + 1 + ackSizeInBytes);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName");
            UIO.write(filer, context, "context");
            UIO.writeLong(filer, partitionVersion, "partitionVersion");
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember");
            UIO.writeBoolean(filer, !rootRingMember.equals(ackRingMember), "isOther");
            UIO.writeByteArray(filer, ackRingMember.getMember(), "ackRingMember");
            return filer.getBytes();
        } else if (rootRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + 8 + rootSizeInBytes);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName");
            UIO.write(filer, context, "context");
            UIO.writeLong(filer, partitionVersion, "partitionVersion");
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember");
            return filer.getBytes();
        } else {
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + 8);
            UIO.writeByteArray(filer, partitionName.toBytes(), "versionedPartitionName");
            UIO.write(filer, context, "context");
            return filer.getBytes();
        }
    }

    private static boolean streamStateKey(byte[] keyBytes, StateKeyStream stream) throws Exception {
        HeapFiler filer = new HeapFiler(keyBytes);
        byte[] partitionNameBytes = UIO.readByteArray(filer, "partitionName");
        byte context = UIO.readByte(filer, "context");
        long partitionVersion = UIO.readLong(filer, "partitionVersion");
        boolean isSelf = !UIO.readBoolean(filer, "isOther");
        byte[] rootRingMemberBytes = UIO.readByteArray(filer, "rootRingMember");
        byte[] ackRingMemberBytes = UIO.readByteArray(filer, "ackRingMember");
        return stream.stream(PartitionName.fromBytes(partitionNameBytes),
            context,
            partitionVersion,
            new Member(rootRingMemberBytes),
            isSelf,
            new Member(ackRingMemberBytes));
    }

    interface StateKeyStream {
        boolean stream(PartitionName partitionName,
            byte context,
            long partitionVersion,
            Member rootRingMember,
            boolean isSelf,
            Member ackRingMember) throws Exception;
    }

    private static byte[] livelinessKey(Member rootRingMember, Member ackRingMember) throws Exception {
        Preconditions.checkNotNull(rootRingMember, "Requires root ring member");
        if (ackRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            int ackSizeInBytes = 4 + ackRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(rootSizeInBytes + 1 + ackSizeInBytes);
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember");
            UIO.writeBoolean(filer, !rootRingMember.equals(ackRingMember), "isOther");
            UIO.writeByteArray(filer, ackRingMember.getMember(), "ackRingMember");
            return filer.getBytes();
        } else {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(rootSizeInBytes);
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember");
            return filer.getBytes();
        }
    }

    private static boolean streamLivelinessKey(byte[] keyBytes, LivelinessKeyStream stream) throws Exception {
        HeapFiler filer = new HeapFiler(keyBytes);
        boolean isSelf = !UIO.readBoolean(filer, "isOther");
        byte[] rootRingMemberBytes = UIO.readByteArray(filer, "rootRingMember");
        byte[] ackRingMemberBytes = UIO.readByteArray(filer, "ackRingMember");
        return stream.stream(new Member(rootRingMemberBytes),
            isSelf,
            new Member(ackRingMemberBytes));
    }

    interface LivelinessKeyStream {
        boolean stream(Member rootRingMember, boolean isSelf, Member ackRingMember) throws Exception;
    }

    private static class AmzaStateStorage implements StateStorage<Long> {

        private final SystemWALStorage systemWALStorage;
        private final StorageVersionProvider storageVersionProvider;
        private final AmzaRingReader amzaRingReader;
        private final WALUpdated walUpdated;
        private final Member member;
        private final VersionedPartitionName versionedPartitionName;
        private final byte context;

        public AmzaStateStorage(SystemWALStorage systemWALStorage,
            StorageVersionProvider storageVersionProvider,
            AmzaRingReader amzaRingReader,
            WALUpdated walUpdated,
            Member member,
            VersionedPartitionName versionedPartitionName,
            byte context) {
            this.systemWALStorage = systemWALStorage;
            this.storageVersionProvider = storageVersionProvider;
            this.amzaRingReader = amzaRingReader;
            this.walUpdated = walUpdated;
            this.member = member;
            this.versionedPartitionName = versionedPartitionName;
            this.context = context;
        }

        @Override
        public boolean scan(Member rootMember, Member otherMember, Long lifecycle, StateStream<Long> stream) throws Exception {
            byte[] fromKey = stateKey(versionedPartitionName.getPartitionName(), context, lifecycle, rootMember, otherMember);
            return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_STATE_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
                (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    if (valueTimestamp != -1 && !valueTombstoned) {
                        return streamStateKey(key, (partitionName, context, partitionVersion, rootRingMember, isSelf, ackRingMember) -> {
                            State state = State.fromSerializedForm(value[0]);
                            return stream.stream(rootRingMember, isSelf, ackRingMember, partitionVersion,
                                state, valueTimestamp, valueVersion);
                        });
                    }
                    return true;
                });
        }

        @Override
        public boolean update(StateUpdates<Long> updates) throws Exception {
            AmzaPartitionUpdates amzaPartitionUpdates = new AmzaPartitionUpdates();
            boolean result = updates.updates((rootMember, otherMember, lifecycle, state, timestamp) -> {
                byte[] keyBytes = stateKey(versionedPartitionName.getPartitionName(), context, lifecycle, rootMember, otherMember);
                byte[] valueBytes = { state.getSerializedForm() };
                amzaPartitionUpdates.set(keyBytes, valueBytes, timestamp);
                return true;
            });
            if (result) {
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, amzaPartitionUpdates, walUpdated);
                return !rowsChanged.isEmpty();
            } else {
                return false;
            }
        }
    }

    private static class AmzaLivelinessStorage implements LivelinessStorage {

        private final SystemWALStorage systemWALStorage;
        private final WALUpdated walUpdated;
        private final Member member;

        public AmzaLivelinessStorage(SystemWALStorage systemWALStorage, WALUpdated walUpdated, Member member) {
            this.systemWALStorage = systemWALStorage;
            this.member = member;
            this.walUpdated = walUpdated;
        }

        @Override
        public boolean scan(Member rootMember, Member otherMember, LivelinessStream stream) throws Exception {
            byte[] fromKey = livelinessKey(member, null);
            return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
                (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                    if (valueTimestamp != -1 && !valueTombstoned) {
                        return streamLivelinessKey(key, (rootRingMember, isSelf, ackRingMember) ->
                            stream.stream(rootRingMember, isSelf, ackRingMember, valueTimestamp, valueVersion));
                    }
                    return true;
                });
        }

        @Override
        public boolean update(LivelinessUpdates updates) throws Exception {
            AmzaPartitionUpdates amzaPartitionUpdates = new AmzaPartitionUpdates();
            boolean result = updates.updates((rootMember, otherMember, timestamp) -> {
                byte[] keyBytes = livelinessKey(rootMember, otherMember);
                amzaPartitionUpdates.set(keyBytes, new byte[0], timestamp);
                return true;
            });
            if (result) {
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, null, amzaPartitionUpdates, walUpdated);
                return !rowsChanged.isEmpty();
            } else {
                return false;
            }
        }

        @Override
        public long get(Member rootMember, Member otherMember) throws Exception {
            TimestampedValue timestampedValue = systemWALStorage.getTimestampedValue(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, null,
                livelinessKey(rootMember, otherMember));
            return timestampedValue != null ? timestampedValue.getTimestampId() : -1;
        }
    }

    private static class AmzaMemberLifecycle implements MemberLifecycle<Long> {

        private final StorageVersionProvider storageVersionProvider;
        private final VersionedPartitionName versionedPartitionName;

        public AmzaMemberLifecycle(StorageVersionProvider storageVersionProvider, VersionedPartitionName versionedPartitionName) {
            this.storageVersionProvider = storageVersionProvider;
            this.versionedPartitionName = versionedPartitionName;
        }

        @Override
        public Long get() throws Exception {
            StorageVersion storageVersion = storageVersionProvider.get(versionedPartitionName.getPartitionName());
            return storageVersion != null ? storageVersion.partitionVersion : null;
        }

        @Override
        public Long getOther(Member other) throws Exception {
            StorageVersion storageVersion = storageVersionProvider.getRemote(RingMember.fromAquariumMember(other), versionedPartitionName.getPartitionName());
            return storageVersion != null ? storageVersion.partitionVersion : null;
        }
    }

    private static class AmzaRingSize implements RingSize {

        private final AmzaRingReader amzaRingReader;
        private final VersionedPartitionName versionedPartitionName;

        public AmzaRingSize(AmzaRingReader amzaRingReader, VersionedPartitionName versionedPartitionName) {
            this.amzaRingReader = amzaRingReader;
            this.versionedPartitionName = versionedPartitionName;
        }

        @Override
        public int get() throws Exception {
            return amzaRingReader.getRingSize(versionedPartitionName.getPartitionName().getRingName());
        }
    }
}
