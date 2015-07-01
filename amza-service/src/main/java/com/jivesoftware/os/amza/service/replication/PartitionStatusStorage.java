package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionTx;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class PartitionStatusStorage implements TxPartitionStatus {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final SystemWALStorage systemWALStorage;
    private final AmzaRingReader amzaRingReader;
    private final TakeCoordinator takeCoordinator;
    private final WALUpdated walUpdated;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final Map<VersionedPartitionName, VersionedStatus> localStatusCache = Maps.newConcurrentMap();
    private final ConcurrentHashMap<PartitionName, ConcurrentHashMap<RingMember, VersionedStatus>> remoteStatusCache = new ConcurrentHashMap<>();

    public PartitionStatusStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        AmzaRingReader amzaRingReader,
        TakeCoordinator takeCoordinator,
        int awaitOnlineStripingLevel) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemWALStorage = systemWALStorage;
        this.walUpdated = walUpdated;
        this.amzaRingReader = amzaRingReader;
        this.takeCoordinator = takeCoordinator;
        this.transactor = new VersionedPartitionTransactor(1024, 1024); // TODO expose to config?
        this.awaitNotify = new AwaitNotify<>(awaitOnlineStripingLevel);
    }

    WALKey walKey(RingMember member, PartitionName partitionName) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "serializationVersion");
        UIO.writeByteArray(filer, member.toBytes(), "member");
        if (partitionName != null) {
            UIO.writeByteArray(filer, partitionName.toBytes(), "partition");
        }
        return new WALKey(filer.getBytes());
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        if (partitionName.isSystemPartition()) {
            return tx.tx(new VersionedPartitionName(partitionName, 0), Status.ONLINE);
        }

        WALValue rawStatus = systemWALStorage.get(PartitionProvider.REGION_ONLINE_INDEX, walKey(rootRingMember, partitionName));
        if (rawStatus == null) {
            return tx.tx(null, null);
        } else {
            VersionedStatus versionedStatus = VersionedStatus.fromBytes(rawStatus.getValue());
            return transactor.doWithOne(new VersionedPartitionName(partitionName, versionedStatus.version), versionedStatus.status, tx);
        }
    }

    public void remoteStatus(RingMember remoteRingMember, PartitionName partitionName, VersionedStatus remoteVersionedStatus) {

        ConcurrentHashMap<RingMember, VersionedStatus> ringMemberStatus = remoteStatusCache.computeIfAbsent(partitionName,
            (key) -> {
                return new ConcurrentHashMap<>();
            });

        ringMemberStatus.merge(remoteRingMember, remoteVersionedStatus, (existing, updated) -> {
            return (updated.version > existing.version) ? updated : existing;
        });

    }

    public void elect(Collection<RingMember> remoteRingMembers, VersionedPartitionName localVersionedPartitionName) throws Exception {

        ConcurrentHashMap<RingMember, VersionedStatus> ringMemberStatus = remoteStatusCache.get(localVersionedPartitionName.getPartitionName());
        if (ringMemberStatus != null) {
            int inKetchup = 0;
            for (RingMember ringMember : remoteRingMembers) {
                VersionedStatus remoteRingMemberStatus = ringMemberStatus.get(ringMember);
                if (remoteRingMemberStatus == null) {
                    remoteRingMemberStatus = getStatus(ringMember, localVersionedPartitionName.getPartitionName());
                }
                if (remoteRingMemberStatus != null && Status.KETCHUP == remoteRingMemberStatus.status) {
                    inKetchup++;
                }
            }
            if (inKetchup == remoteRingMembers.size()) {
                markAsOnline(localVersionedPartitionName);
                LOG.info("Resolving cold start stalemate. " + rootRingMember + " was elected as online for " + localVersionedPartitionName);
            }
        }
    }

    public VersionedStatus getStatus(RingMember ringMember, PartitionName partitionName) throws Exception {
        WALValue rawStatus = systemWALStorage.get(PartitionProvider.REGION_ONLINE_INDEX, walKey(ringMember, partitionName));
        if (rawStatus == null || rawStatus.getTombstoned()) {
            return null;
        }
        return VersionedStatus.fromBytes(rawStatus.getValue());
    }

    public VersionedStatus markAsKetchup(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedStatus(Status.ONLINE, 0);
        }
        long partitionVersion = orderIdProvider.nextId();
        return set(rootRingMember, partitionName, new VersionedStatus(Status.KETCHUP, partitionVersion));
    }

    public void markAsOnline(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        set(rootRingMember, versionedPartitionName.getPartitionName(), new VersionedStatus(Status.ONLINE, versionedPartitionName.getPartitionVersion()));
    }

    public void markForDisposal(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        set(ringMember, versionedPartitionName.getPartitionName(), new VersionedStatus(Status.EXPUNGE, versionedPartitionName.getPartitionVersion()));
    }

    public void streamLocalState(PartitionMemberStatusStream stream) throws Exception {
        WALKey from = walKey(rootRingMember, null);
        WALKey to = from.prefixUpperExclusive();
        systemWALStorage.rangeScan(PartitionProvider.REGION_ONLINE_INDEX, from, to, (rowTxId, key, value) -> {
            HeapFiler filer = new HeapFiler(key.getKey());
            UIO.readByte(filer, "serializationVersion");
            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member"));
            PartitionName partitionName = PartitionName.fromBytes(UIO.readByteArray(filer, "partition"));

            VersionedStatus versionStatus = VersionedStatus.fromBytes(value.getValue());

            return transactor.doWithOne(new VersionedPartitionName(partitionName, versionStatus.version),
                versionStatus.status,
                (versionedPartitionName, partitionStatus) -> stream.stream(partitionName, ringMember, versionStatus));
        });
    }

    private VersionedStatus set(RingMember ringMember,
        PartitionName partitionName,
        VersionedStatus versionedStatus) throws Exception {

        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
        VersionedStatus cachedStatus = localStatusCache.get(versionedPartitionName);
        if (cachedStatus != null && cachedStatus.equals(versionedStatus)) {
            return versionedStatus;
        }

        return transactor.doWithAll(versionedPartitionName, versionedStatus.status, (currentVersionedPartitionName, status) -> {

            WALValue rawStatus = systemWALStorage.get(PartitionProvider.REGION_ONLINE_INDEX, walKey(rootRingMember, partitionName));
            VersionedStatus commitableVersionStatus = null;
            VersionedStatus returnableStatus = null;

            if ((rawStatus == null || rawStatus.getTombstoned())) {
                if (versionedStatus.status == Status.KETCHUP) {
                    commitableVersionStatus = versionedStatus;
                    returnableStatus = versionedStatus;
                }
            } else {
                VersionedStatus currentVersionedStatus = VersionedStatus.fromBytes(rawStatus.getValue());
                if (currentVersionedStatus.version == versionedStatus.version && isValidTransition(currentVersionedStatus, versionedStatus)) {
                    commitableVersionStatus = versionedStatus;
                    returnableStatus = versionedStatus;
                } else {
                    returnableStatus = currentVersionedStatus;
                }

            }
            if (commitableVersionStatus != null) {
                byte[] versionedStatusBytes = commitableVersionStatus.toBytes();
                awaitNotify.notifyChange(partitionName, () -> {
                    RowsChanged rowsChanged = systemWALStorage.update(PartitionProvider.REGION_ONLINE_INDEX,
                        (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                            walKey(ringMember, partitionName),
                            new WALValue(versionedStatusBytes, orderIdProvider.nextId(), false)), walUpdated);
                    return !rowsChanged.isEmpty();
                });
                LOG.info("STATUS {}: {} versionedPartitionName:{} was updated to {}",
                    rootRingMember, ringMember, versionedPartitionName, commitableVersionStatus);
                localStatusCache.put(currentVersionedPartitionName, commitableVersionStatus);
                takeCoordinator.updated(amzaRingReader, versionedPartitionName, commitableVersionStatus.status, 0);
                takeCoordinator.awakeCya();
            }
            return returnableStatus;
        });

    }

    private static boolean isValidTransition(VersionedStatus currentVersionedStatus, VersionedStatus versionedStatus) {
        return (currentVersionedStatus.status == Status.KETCHUP && versionedStatus.status == Status.ONLINE)
            || (currentVersionedStatus.status == Status.ONLINE && versionedStatus.status == Status.EXPUNGE);
    }

    public void expunged(List<VersionedPartitionName> composted) throws Exception {
        for (VersionedPartitionName compost : composted) {
            transactor.doWithAll(compost, Status.EXPUNGE, (versionedPartitionName, partitionStatus) -> {
                awaitNotify.notifyChange(compost.getPartitionName(), () -> {
                    RowsChanged rowsChanged = systemWALStorage.update(PartitionProvider.REGION_ONLINE_INDEX,
                        (highwaters, scan) -> {
                            scan.row(orderIdProvider.nextId(),
                                walKey(rootRingMember, compost.getPartitionName()),
                                new WALValue(null, orderIdProvider.nextId(), true));
                        }, walUpdated);
                    return !rowsChanged.isEmpty();
                });
                localStatusCache.remove(compost);
                return null;
            });
        }
    }

    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        awaitNotify.awaitChange(partitionName, () -> {
            PartitionStatusStorage.VersionedStatus versionedStatus = getStatus(rootRingMember, partitionName);
            if (versionedStatus != null) {
                if (versionedStatus.status == TxPartitionStatus.Status.EXPUNGE) {
                    throw new IllegalStateException("Partition is being expunged");
                } else if (versionedStatus.status == TxPartitionStatus.Status.ONLINE) {
                    return Optional.absent();
                }
            }
            return null;
        }, timeoutMillis);
    }

    public interface PartitionMemberStatusStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedStatus versionedStatus) throws Exception;
    }

    static public class VersionedStatus {

        public final Status status;
        public final long version;

        public byte[] toBytes() throws IOException {
            HeapFiler filer = new HeapFiler();
            UIO.writeByte(filer, 0, "serializationVersion");
            UIO.writeByteArray(filer, status.getSerializedForm(), "status");
            UIO.writeLong(filer, version, "version");
            return filer.getBytes();
        }

        public static VersionedStatus fromBytes(byte[] bytes) throws IOException {
            HeapFiler filer = new HeapFiler(bytes);
            byte serializationVersion = UIO.readByte(filer, "serializationVersion");
            if (serializationVersion != 0) {
                throw new IllegalStateException("Failed to deserialize due to an unknown version:" + serializationVersion);
            }
            Status status = Status.fromSerializedForm(UIO.readByteArray(filer, "status"));
            long version = UIO.readLong(filer, "version");
            return new VersionedStatus(status, version);
        }

        VersionedStatus(Status status, long version) {
            Preconditions.checkNotNull(status, "Status cannot be null");
            this.status = status;
            this.version = version;
        }

        @Override
        public String toString() {
            return "VersionedStatus{"
                + "status=" + status
                + ", version=" + version
                + '}';
        }
    }
}
