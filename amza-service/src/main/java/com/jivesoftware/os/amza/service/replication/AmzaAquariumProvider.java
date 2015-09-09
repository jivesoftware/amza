package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.StorageVersion;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.aquarium.Aquarium;
import com.jivesoftware.os.amza.aquarium.AtQuorum;
import com.jivesoftware.os.amza.aquarium.Liveliness;
import com.jivesoftware.os.amza.aquarium.LivelinessStorage;
import com.jivesoftware.os.amza.aquarium.Member;
import com.jivesoftware.os.amza.aquarium.MemberLifecycle;
import com.jivesoftware.os.amza.aquarium.ReadWaterline;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.stream.KeyValueStream;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaAquariumProvider implements RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;

    private final RingMember rootRingMember;
    private final Member rootAquariumMember;
    private final OrderIdProvider orderIdProvider;
    private final AmzaRingReader amzaRingReader;
    private final SystemWALStorage systemWALStorage;
    private final StorageVersionProvider storageVersionProvider;
    private final WALUpdated walUpdated;
    private final long startupVersion;

    private final Liveliness liveliness;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Set<VersionedPartitionName> smellsFishy = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public AmzaAquariumProvider(long startupVersion,
        RingMember rootRingMember,
        OrderIdProvider orderIdProvider,
        AmzaRingReader amzaRingReader,
        SystemWALStorage systemWALStorage,
        StorageVersionProvider storageVersionProvider,
        WALUpdated walUpdated,
        Liveliness liveliness) {
        this.startupVersion = startupVersion;
        this.rootRingMember = rootRingMember;
        this.rootAquariumMember = rootRingMember.asAquariumMember();
        this.orderIdProvider = orderIdProvider;
        this.amzaRingReader = amzaRingReader;
        this.systemWALStorage = systemWALStorage;
        this.storageVersionProvider = storageVersionProvider;
        this.walUpdated = walUpdated;
        this.liveliness = liveliness;
    }

    public void start(long feedEveryMillis) {
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                liveliness.feedTheFish();
                Iterator<VersionedPartitionName> iter = smellsFishy.iterator();
                while (iter.hasNext()) {
                    VersionedPartitionName versionedPartitionName = iter.next();
                    iter.remove();
                    StorageVersion storageVersion = storageVersionProvider.createIfAbsent(versionedPartitionName.getPartitionName());
                    getAquarium(new VersionedPartitionName(versionedPartitionName.getPartitionName(), storageVersion.partitionVersion)).tapTheGlass();
                }
            } catch (Exception e) {
                LOG.error("Failed to feed the fish", e);
            }
        }, 0, feedEveryMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduledExecutorService.shutdownNow();
    }

    public void tookFully(RingMember fromRingMember, VersionedPartitionName versionedPartitionName) throws Exception {
    }

    public Aquarium getAquarium(VersionedPartitionName versionedPartitionName) throws Exception {
        AtQuorum atQuorum = new AmzaAtQuorum(amzaRingReader, versionedPartitionName);
        ReadWaterline readCurrent = new ReadWaterline<>(
            new AmzaStateStorage(systemWALStorage, walUpdated, rootAquariumMember, versionedPartitionName, CURRENT, startupVersion),
            liveliness,
            new AmzaMemberLifecycle(storageVersionProvider, versionedPartitionName),
            atQuorum,
            rootRingMember.asAquariumMember(),
            Long.class);

        ReadWaterline readDesired = new ReadWaterline<>(
            new AmzaStateStorage(systemWALStorage, walUpdated, rootAquariumMember, versionedPartitionName, DESIRED, startupVersion),
            liveliness,
            new AmzaMemberLifecycle(storageVersionProvider, versionedPartitionName),
            atQuorum,
            rootRingMember.asAquariumMember(),
            Long.class);

        return new Aquarium(orderIdProvider,
            System::currentTimeMillis,
            (member, tx) -> tx.tx(readCurrent, readDesired),
            (current, desiredTimestamp, state) -> {
                //TODO make sure this is a valid transition
                byte[] keyBytes = stateKey(versionedPartitionName.getPartitionName(), CURRENT, rootAquariumMember, versionedPartitionName.getPartitionVersion(),
                    rootAquariumMember);
                byte[] valueBytes = { state.getSerializedForm() };
                AmzaPartitionUpdates updates = new AmzaPartitionUpdates().set(keyBytes, valueBytes, desiredTimestamp);
                LOG.info("Current {} for {} = {}", rootAquariumMember, versionedPartitionName, state);
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, updates, walUpdated);
                return !rowsChanged.isEmpty();
            },
            (current, desiredTimestamp, state) -> {
                //TODO make sure this is a valid transition
                byte[] keyBytes = stateKey(versionedPartitionName.getPartitionName(), DESIRED, rootAquariumMember, versionedPartitionName.getPartitionVersion(),
                    rootAquariumMember);
                byte[] valueBytes = { state.getSerializedForm() };
                AmzaPartitionUpdates updates = new AmzaPartitionUpdates().set(keyBytes, valueBytes, desiredTimestamp);
                LOG.info("Desired {} for {} = {}", rootAquariumMember, versionedPartitionName, state);
                RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.AQUARIUM_STATE_INDEX, null, updates, walUpdated);
                return !rowsChanged.isEmpty();
            },
            rootRingMember.asAquariumMember());
    }

    static byte[] stateKey(PartitionName partitionName,
        byte context,
        Member rootRingMember,
        Long partitionVersion,
        Member ackRingMember) throws Exception {

        int partitionSizeInBytes = 4 + partitionName.sizeInBytes();
        if (rootRingMember != null && ackRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            int ackSizeInBytes = 4 + ackRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + rootSizeInBytes + 8 + 1 + ackSizeInBytes);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName");
            UIO.write(filer, context, "context");
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember");
            UIO.writeLong(filer, partitionVersion, "partitionVersion");
            UIO.writeBoolean(filer, !rootRingMember.equals(ackRingMember), "isOther");
            UIO.writeByteArray(filer, ackRingMember.getMember(), "ackRingMember");
            return filer.getBytes();
        } else if (rootRingMember != null) {
            int rootSizeInBytes = 4 + rootRingMember.getMember().length;
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1 + rootSizeInBytes + 8);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName");
            UIO.write(filer, context, "context");
            UIO.writeByteArray(filer, rootRingMember.getMember(), "rootRingMember");
            UIO.writeLong(filer, partitionVersion, "partitionVersion");
            return filer.getBytes();
        } else {
            HeapFiler filer = new HeapFiler(partitionSizeInBytes + 1);
            UIO.writeByteArray(filer, partitionName.toBytes(), "partitionName");
            UIO.write(filer, context, "context");
            return filer.getBytes();
        }
    }

    static boolean streamStateKey(byte[] keyBytes, StateKeyStream stream) throws Exception {
        HeapFiler filer = new HeapFiler(keyBytes);
        byte[] partitionNameBytes = UIO.readByteArray(filer, "partitionName");
        byte context = UIO.readByte(filer, "context");
        byte[] rootRingMemberBytes = UIO.readByteArray(filer, "rootRingMember");
        long partitionVersion = UIO.readLong(filer, "partitionVersion");
        boolean isSelf = !UIO.readBoolean(filer, "isOther");
        byte[] ackRingMemberBytes = UIO.readByteArray(filer, "ackRingMember");
        return stream.stream(PartitionName.fromBytes(partitionNameBytes),
            context,
            new Member(rootRingMemberBytes), partitionVersion,
            isSelf,
            new Member(ackRingMemberBytes));
    }

    interface StateKeyStream {

        boolean stream(PartitionName partitionName,
            byte context,
            Member rootRingMember, long partitionVersion,
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
        byte[] rootRingMemberBytes = UIO.readByteArray(filer, "rootRingMember");
        boolean isSelf = !UIO.readBoolean(filer, "isOther");
        byte[] ackRingMemberBytes = UIO.readByteArray(filer, "ackRingMember");
        return stream.stream(new Member(rootRingMemberBytes),
            isSelf,
            new Member(ackRingMemberBytes));
    }

    interface LivelinessKeyStream {

        boolean stream(Member rootRingMember, boolean isSelf, Member ackRingMember) throws Exception;
    }


    public static class AmzaLivelinessStorage implements LivelinessStorage {

        private final SystemWALStorage systemWALStorage;
        private final WALUpdated walUpdated;
        private final Member member;
        private final long startupVersion;

        public AmzaLivelinessStorage(SystemWALStorage systemWALStorage, WALUpdated walUpdated, Member member, long startupVersion) {
            this.systemWALStorage = systemWALStorage;
            this.member = member;
            this.walUpdated = walUpdated;
            this.startupVersion = startupVersion;
        }

        @Override
        public boolean scan(Member rootMember, Member otherMember, LivelinessStream stream) throws Exception {
            KeyValueStream keyValueStream = (prefix, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                if (valueTimestamp != -1 && !valueTombstoned) {
                    return streamLivelinessKey(key, (rootRingMember, isSelf, ackRingMember) -> {
                        if (!rootRingMember.equals(member) || valueVersion > startupVersion) {
                            return stream.stream(rootRingMember, isSelf, ackRingMember, valueTimestamp, valueVersion);
                        } else {
                            return true;
                        }
                    });
                }
                return true;
            };

            if (rootMember == null && otherMember == null) {
                return systemWALStorage.rowScan(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, keyValueStream);
            } else {
                byte[] fromKey = livelinessKey(rootMember, otherMember);
                return systemWALStorage.rangeScan(PartitionCreator.AQUARIUM_LIVELINESS_INDEX, null, fromKey, null, WALKey.prefixUpperExclusive(fromKey),
                    keyValueStream);
            }
        }

        @Override
        public boolean update(LivelinessUpdates updates) throws Exception {
            AmzaPartitionUpdates amzaPartitionUpdates = new AmzaPartitionUpdates();
            boolean result = updates.updates((rootMember, otherMember, timestamp) -> {
                byte[] keyBytes = livelinessKey(rootMember, otherMember);
                amzaPartitionUpdates.set(keyBytes, new byte[0], timestamp);
                return true;
            });
            if (result && amzaPartitionUpdates.size() > 0) {
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

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.AQUARIUM_STATE_INDEX.equals(changes.getVersionedPartitionName())) {
            for (Map.Entry<WALKey, WALValue> change : changes.getApply().entrySet()) {
                streamStateKey(change.getKey().key, (partitionName, context, rootRingMember1, partitionVersion, isSelf, ackRingMember) -> {
                    smellsFishy.add(new VersionedPartitionName(partitionName, partitionVersion));
                    return true;
                });
            }
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
            StorageVersion storageVersion = storageVersionProvider.createIfAbsent(versionedPartitionName.getPartitionName());
            return storageVersion.partitionVersion;
        }

        @Override
        public Long getOther(Member other) throws Exception {
            StorageVersion storageVersion = storageVersionProvider.getRemote(RingMember.fromAquariumMember(other), versionedPartitionName.getPartitionName());
            return storageVersion != null ? storageVersion.partitionVersion : null;
        }
    }

    private static class AmzaAtQuorum implements AtQuorum {

        private final AmzaRingReader amzaRingReader;
        private final VersionedPartitionName versionedPartitionName;

        public AmzaAtQuorum(AmzaRingReader amzaRingReader, VersionedPartitionName versionedPartitionName) {
            this.amzaRingReader = amzaRingReader;
            this.versionedPartitionName = versionedPartitionName;
        }

        @Override
        public boolean is(int count) throws Exception {
            return count > amzaRingReader.getRingSize(versionedPartitionName.getPartitionName().getRingName()) / 2;
        }
    }
}
