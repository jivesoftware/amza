package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionTx;
import com.jivesoftware.os.amza.shared.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class PartitionStatusStorage implements TxPartitionStatus {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final PartitionStripe systemPartitionStripe;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final Map<VersionedPartitionName, VersionedStatus> localStatusCache = Maps.newConcurrentMap();

    public PartitionStatusStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        PartitionStripe systemPartitionStripe,
        int awaitOnlineStripingLevel) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemPartitionStripe = systemPartitionStripe;
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

        WALValue rawStatus = systemPartitionStripe.get(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(), walKey(rootRingMember, partitionName));
        if (rawStatus == null) {
            return tx.tx(null, null);
        } else {
            VersionedStatus versionedStatus = VersionedStatus.fromBytes(rawStatus.getValue());
            return transactor.doWithOne(new VersionedPartitionName(partitionName, versionedStatus.version), versionedStatus.status, tx);
        }
    }

    public void elect(Collection<RingMember> ringMembers, Set<RingMember> membersUnreachable, VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition() || ringMembers.isEmpty()) {
            return;
        }
        for (RingMember ringMember : ringMembers) {
            WALKey key = walKey(ringMember, versionedPartitionName.getPartitionName());
            WALValue rawStatus = systemPartitionStripe.get(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(), key);
            if (rawStatus == null || rawStatus.getTombstoned()) {
                if (membersUnreachable.contains(ringMember)) {
                    continue;
                } else {
                    return;
                }
            }
            VersionedStatus versionedStatus = VersionedStatus.fromBytes(rawStatus.getValue());
            if (versionedStatus.status == Status.ONLINE) {
                return;
            }
        }
        LOG.info("Resolving cold start stalemate. " + rootRingMember);
        markAsOnline(versionedPartitionName);
    }

    public VersionedStatus getStatus(RingMember ringMember, PartitionName partitionName) throws Exception {
        WALValue rawStatus = systemPartitionStripe.get(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(), walKey(ringMember, partitionName));
        if (rawStatus == null || rawStatus.getTombstoned()) {
            return null;
        }
        return VersionedStatus.fromBytes(rawStatus.getValue());
    }

    public VersionedPartitionName markAsKetchup(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedPartitionName(partitionName, 0);
        }
        long partitionVersion = orderIdProvider.nextId();
        VersionedStatus versionedStatus = set(rootRingMember, partitionName, new VersionedStatus(Status.KETCHUP, partitionVersion));
        return new VersionedPartitionName(partitionName, versionedStatus.version);
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
        systemPartitionStripe.rangeScan(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(), from, to, (rowTxId, key, value) -> {
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

    private VersionedStatus set(RingMember ringMember, PartitionName partitionName, VersionedStatus versionedStatus) throws Exception {
        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
        VersionedStatus cachedStatus = localStatusCache.get(versionedPartitionName);
        if (cachedStatus != null && cachedStatus.equals(versionedStatus)) {
            return versionedStatus;
        }

        return transactor.doWithAll(versionedPartitionName, versionedStatus.status, (currentVersionedPartitionName, status) -> {

            WALValue rawStatus = systemPartitionStripe.get(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(), walKey(rootRingMember, partitionName));
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
                    RowsChanged rowsChanged = systemPartitionStripe.commit(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(),
                        Optional.absent(),
                        false,
                        (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                            walKey(ringMember, partitionName),
                            new WALValue(versionedStatusBytes, orderIdProvider.nextId(), false)));
                    return !rowsChanged.isEmpty();
                });
                LOG.info("{}: {} versionedPartitionName:{} was updated to {}", rootRingMember, ringMember, versionedPartitionName, commitableVersionStatus);
                localStatusCache.put(currentVersionedPartitionName, commitableVersionStatus);
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
                    RowsChanged rowsChanged = systemPartitionStripe.commit(PartitionProvider.REGION_ONLINE_INDEX.getPartitionName(),
                        Optional.absent(),
                        false,
                        (highwaters, scan) -> {
                            scan.row(orderIdProvider.nextId(),
                                walKey(rootRingMember, compost.getPartitionName()),
                                new WALValue(null, orderIdProvider.nextId(), true));
                        });
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
            this.status = status;
            this.version = version;
        }

        @Override
        public String toString() {
            return "VersionedStatus{" +
                "status=" + status +
                ", version=" + version +
                '}';
        }
    }
}
