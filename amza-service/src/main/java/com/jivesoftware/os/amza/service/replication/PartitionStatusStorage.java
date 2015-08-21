package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus;
import com.jivesoftware.os.amza.api.partition.TxPartitionStatus.Status;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedStatus;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider.PartitionStripeFunction;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedStatus;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionTransactor;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.scan.RowChanges;
import com.jivesoftware.os.amza.shared.scan.RowsChanged;
import com.jivesoftware.os.amza.shared.take.TakeCoordinator;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALUpdated;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class PartitionStatusStorage implements TxPartitionStatus, RowChanges {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final SystemWALStorage systemWALStorage;
    private final AmzaRingReader amzaRingReader;
    private final TakeCoordinator takeCoordinator;
    private final PartitionStripeFunction partitionStripeFunction;
    private final long[] stripeVersions;
    private final WALUpdated walUpdated;
    private final VersionedPartitionTransactor transactor;
    private final AwaitNotify<PartitionName> awaitNotify;

    private final Map<PartitionName, VersionedStatus> localStatusCache = Maps.newConcurrentMap();
    private final ConcurrentHashMap<PartitionName, ConcurrentHashMap<RingMember, RemoteVersionedStatus>> remoteStatusCache = new ConcurrentHashMap<>();

    public PartitionStatusStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        SystemWALStorage systemWALStorage,
        WALUpdated walUpdated,
        AmzaRingReader amzaRingReader,
        TakeCoordinator takeCoordinator,
        PartitionStripeFunction partitionStripeFunction,
        long[] stripeVersions,
        int awaitOnlineStripingLevel) {
        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemWALStorage = systemWALStorage;
        this.walUpdated = walUpdated;
        this.amzaRingReader = amzaRingReader;
        this.takeCoordinator = takeCoordinator;
        this.partitionStripeFunction = partitionStripeFunction;
        this.stripeVersions = stripeVersions;
        this.transactor = new VersionedPartitionTransactor(1024, 1024); // TODO expose to config?
        this.awaitNotify = new AwaitNotify<>(awaitOnlineStripingLevel);
    }

    byte[] walKey(RingMember member, PartitionName partitionName) throws Exception {
        byte[] memberBytes = member.toBytes();
        if (partitionName != null) {
            byte[] partitionNameBytes = partitionName.toBytes();
            byte[] asBytes = new byte[1 + 4 + memberBytes.length + 4 + partitionNameBytes.length];
            asBytes[0] = 0; // version
            UIO.intBytes(memberBytes.length, asBytes, 1);
            System.arraycopy(memberBytes, 0, asBytes, 1 + 4, memberBytes.length);
            UIO.intBytes(partitionNameBytes.length, asBytes, 1 + 4 + memberBytes.length);
            System.arraycopy(partitionNameBytes, 0, asBytes, 1 + 4 + memberBytes.length + 4, partitionNameBytes.length);
            return asBytes;
        } else {
            byte[] asBytes = new byte[1 + 4 + memberBytes.length];
            asBytes[0] = 0; // version
            UIO.intBytes(memberBytes.length, asBytes, 1);
            System.arraycopy(memberBytes, 0, asBytes, 1 + 4, memberBytes.length);
            return asBytes;
        }
    }

    void clearCache(byte[] walKey, byte[] walValue) throws Exception {
        HeapFiler filer = new HeapFiler(walKey);
        UIO.readByte(filer, "serializationVersion");
        RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member"));
        if (ringMember != null) {
            PartitionName partitionName = PartitionName.fromBytes(UIO.readByteArray(filer, "partition"));
            VersionedStatus versionedStatus = VersionedStatus.fromBytes(walValue);
            if (ringMember.equals(rootRingMember)) {
                invalidateLocalStatusCache(new VersionedPartitionName(partitionName, versionedStatus.version));
            } else {
                remoteStatusCache.computeIfPresent(partitionName, (PartitionName key, ConcurrentHashMap<RingMember, RemoteVersionedStatus> cache) -> {
                    cache.remove(ringMember);
                    return cache;
                });
            }
        }
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        if (partitionName.isSystemPartition()) {
            return tx.tx(new VersionedPartitionName(partitionName, 0), Status.ONLINE);
        }

        VersionedStatus versionedStatus = getLocalStatus(partitionName);
        if (versionedStatus == null) {
            return tx.tx(null, null);
        } else {
            return transactor.doWithOne(new VersionedPartitionName(partitionName, versionedStatus.version), versionedStatus.status, tx);
        }
    }

    public void remoteStatus(RingMember remoteRingMember, PartitionName partitionName, RemoteVersionedStatus remoteVersionedStatus) {

        ConcurrentHashMap<RingMember, RemoteVersionedStatus> ringMemberStatus = remoteStatusCache.computeIfAbsent(partitionName,
            (key) -> {
                return new ConcurrentHashMap<>();
            });

        ringMemberStatus.merge(remoteRingMember, remoteVersionedStatus,
            (com.jivesoftware.os.amza.shared.partition.RemoteVersionedStatus existing, com.jivesoftware.os.amza.shared.partition.RemoteVersionedStatus
                updated) -> {
                return (updated.version > existing.version) ? updated : existing;
            });

    }

    public void elect(Collection<RingMember> remoteRingMembers, VersionedPartitionName localVersionedPartitionName) throws Exception {

        ConcurrentHashMap<RingMember, RemoteVersionedStatus> ringMemberStatus = remoteStatusCache.get(localVersionedPartitionName.getPartitionName());
        if (ringMemberStatus != null) {
            int inKetchup = 0;
            for (RingMember ringMember : remoteRingMembers) {
                RemoteVersionedStatus remoteRingMemberStatus = ringMemberStatus.get(ringMember);
                if (remoteRingMemberStatus == null) {
                    remoteRingMemberStatus = getRemoteStatus(ringMember, localVersionedPartitionName.getPartitionName());
                }
                if (remoteRingMemberStatus != null && Status.KETCHUP == remoteRingMemberStatus.status) {
                    inKetchup++;
                }
            }
            if (inKetchup == remoteRingMembers.size()) {
                markAsOnline(localVersionedPartitionName);
                LOG.info(
                    "Resolving cold start stalemate. " + rootRingMember + " was elected as online for " + localVersionedPartitionName
                        + " ring size (" + remoteRingMembers.size() + ")");
            }
        }
    }

    @Override
    public VersionedStatus getLocalStatus(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedStatus(Status.ONLINE, 0, 0);
        }

        VersionedStatus versionedStatus = localStatusCache.get(partitionName);
        if (versionedStatus != null) {
            return versionedStatus;
        }

        TimestampedValue rawStatus = systemWALStorage.getTimestampedValue(PartitionCreator.REGION_ONLINE_INDEX, null, walKey(rootRingMember, partitionName));
        versionedStatus = rawStatus == null ? null : VersionedStatus.fromBytes(rawStatus.getValue());
        if (versionedStatus == null || versionedStatus.stripeVersion != stripeVersions[partitionStripeFunction.stripe(partitionName)]) {
            return null;
        }
        return versionedStatus;
    }

    public RemoteVersionedStatus getRemoteStatus(RingMember ringMember, PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new RemoteVersionedStatus(Status.ONLINE, 0);
        }

        //TODO consider wiring in remote status cache, for now it's racy
        TimestampedValue rawStatus = systemWALStorage.getTimestampedValue(PartitionCreator.REGION_ONLINE_INDEX, null, walKey(ringMember, partitionName));
        if (rawStatus == null) {
            return null;
        }
        VersionedStatus versionedStatus = VersionedStatus.fromBytes(rawStatus.getValue());
        return new RemoteVersionedStatus(versionedStatus.status, versionedStatus.version);
    }

    public VersionedStatus markAsKetchup(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedStatus(Status.ONLINE, 0, 0);
        }
        long partitionVersion = orderIdProvider.nextId();
        return set(rootRingMember, partitionName, new VersionedStatus(Status.KETCHUP,
            partitionVersion,
            stripeVersions[partitionStripeFunction.stripe(partitionName)]));
    }

    public void markAsOnline(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        set(rootRingMember, versionedPartitionName.getPartitionName(), new VersionedStatus(Status.ONLINE,
            versionedPartitionName.getPartitionVersion(),
            stripeVersions[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())]));
    }

    public VersionedStatus markForDisposal(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return new VersionedStatus(Status.ONLINE, 0, 0);
        }
        return set(ringMember, versionedPartitionName.getPartitionName(), new VersionedStatus(Status.EXPUNGE,
            versionedPartitionName.getPartitionVersion(),
            stripeVersions[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())]));
    }

    public void streamLocalState(PartitionMemberStatusStream stream) throws Exception {
        byte[] fromKey = walKey(rootRingMember, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        systemWALStorage.rangeScan(PartitionCreator.REGION_ONLINE_INDEX, null, fromKey, null, toKey, (prefix, key, value, valueTimestamp, valueTombstone) -> {
            HeapFiler filer = new HeapFiler(key);
            UIO.readByte(filer, "serializationVersion");
            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member"));
            PartitionName partitionName = PartitionName.fromBytes(UIO.readByteArray(filer, "partition"));
            VersionedStatus versionStatus = VersionedStatus.fromBytes(value);

            if (versionStatus.stripeVersion == stripeVersions[partitionStripeFunction.stripe(partitionName)]) {
                return transactor.doWithOne(new VersionedPartitionName(partitionName, versionStatus.version),
                    versionStatus.status,
                    (versionedPartitionName, partitionStatus) -> stream.stream(partitionName, ringMember, versionStatus));
            } else {
                return true;
            }

        });
    }

    private VersionedStatus set(RingMember ringMember,
        PartitionName partitionName,
        VersionedStatus versionedStatus) throws Exception {

        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedStatus.version);
        VersionedStatus cachedStatus = localStatusCache.get(partitionName);
        if (cachedStatus != null && cachedStatus.equals(versionedStatus)) {
            return versionedStatus;
        }

        return transactor.doWithAll(versionedPartitionName, versionedStatus.status, (currentVersionedPartitionName, status) -> {

            TimestampedValue rawStatus = systemWALStorage.getTimestampedValue(PartitionCreator.REGION_ONLINE_INDEX, null, walKey(ringMember, partitionName));
            VersionedStatus commitableVersionStatus = null;
            VersionedStatus returnableStatus = null;
            long stripeVersion = stripeVersions[partitionStripeFunction.stripe(partitionName)];
            VersionedStatus currentVersionedStatus = rawStatus == null ? null : VersionedStatus.fromBytes(rawStatus.getValue());
            if (currentVersionedStatus == null || currentVersionedStatus.stripeVersion != stripeVersion) {
                if (versionedStatus.status == Status.KETCHUP) {
                    commitableVersionStatus = versionedStatus;
                    returnableStatus = versionedStatus;
                }
            } else {
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
                    RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.REGION_ONLINE_INDEX, null,
                        (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                            walKey(ringMember, partitionName),
                            versionedStatusBytes, orderIdProvider.nextId(), false), walUpdated);
                    return !rowsChanged.isEmpty();
                });
                LOG.info("STATUS {}: {} versionedPartitionName:{} was updated to {}",
                    rootRingMember, ringMember, versionedPartitionName, commitableVersionStatus);
                if (rootRingMember.equals(ringMember)) {
                    takeCoordinator.statusChanged(amzaRingReader, versionedPartitionName, commitableVersionStatus.status);
                    takeCoordinator.awakeCya();
                }
            }
            if (rootRingMember.equals(ringMember)) {
                if (returnableStatus != null) {
                    localStatusCache.put(partitionName, returnableStatus);
                } else {
                    localStatusCache.remove(partitionName);
                }
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
                    RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.REGION_ONLINE_INDEX, null,
                        (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                            walKey(rootRingMember, compost.getPartitionName()),
                            null, orderIdProvider.nextId(), true),
                        walUpdated);
                    return !rowsChanged.isEmpty();
                });
                invalidateLocalStatusCache(compost);
                return null;
            });
        }
        takeCoordinator.expunged(composted);
    }

    private void invalidateLocalStatusCache(VersionedPartitionName versionedPartitionName) {
        localStatusCache.computeIfPresent(versionedPartitionName.getPartitionName(), (partitionName, versionedStatus) -> {
            if (versionedStatus.version == versionedPartitionName.getPartitionVersion()) {
                return null;
            } else {
                return versionedStatus;
            }
        });
    }

    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        awaitNotify.awaitChange(partitionName, () -> {
            VersionedStatus versionedStatus = getLocalStatus(partitionName);
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

    @Override
    public void changes(RowsChanged changes) throws Exception {
        if (PartitionCreator.REGION_ONLINE_INDEX.equals(changes.getVersionedPartitionName())) {
            for (Entry<WALKey, WALValue> change : changes.getApply().entrySet()) {
                clearCache(change.getKey().key, change.getValue().getValue());
            }
        }
    }

    public interface PartitionMemberStatusStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedStatus versionedStatus) throws Exception;
    }


}
