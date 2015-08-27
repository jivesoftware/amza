package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.TimestampedValue;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionState;
import com.jivesoftware.os.amza.api.partition.PartitionTx;
import com.jivesoftware.os.amza.api.partition.TxPartitionState;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedState;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.service.replication.PartitionStripeProvider.PartitionStripeFunction;
import com.jivesoftware.os.amza.service.storage.PartitionCreator;
import com.jivesoftware.os.amza.service.storage.SystemWALStorage;
import com.jivesoftware.os.amza.shared.AwaitNotify;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.partition.RemoteVersionedState;
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
public class PartitionStateStorage implements TxPartitionState, RowChanges {

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

    private final Map<PartitionName, VersionedState> localStateCache = Maps.newConcurrentMap();
    private final ConcurrentHashMap<PartitionName, ConcurrentHashMap<RingMember, RemoteVersionedState>> remoteStateCache = new ConcurrentHashMap<>();

    public PartitionStateStorage(OrderIdProvider orderIdProvider,
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
            VersionedState versionedState = VersionedState.fromBytes(walValue);
            if (ringMember.equals(rootRingMember)) {
                invalidateLocalStateCache(new VersionedPartitionName(partitionName, versionedState.version));
            } else {
                remoteStateCache.computeIfPresent(partitionName, (PartitionName key, ConcurrentHashMap<RingMember, RemoteVersionedState> cache) -> {
                    cache.remove(ringMember);
                    return cache;
                });
            }
        }
    }

    @Override
    public <R> R tx(PartitionName partitionName, PartitionTx<R> tx) throws Exception {
        if (partitionName.isSystemPartition()) {
            return tx.tx(new VersionedPartitionName(partitionName, 0), PartitionState.ONLINE);
        }

        VersionedState versionedState = getLocalState(partitionName);
        if (versionedState == null) {
            return tx.tx(null, null);
        } else {
            return transactor.doWithOne(new VersionedPartitionName(partitionName, versionedState.version), versionedState.state, tx);
        }
    }

    public void remoteState(RingMember remoteRingMember, PartitionName partitionName, RemoteVersionedState remoteVersionedState) {

        ConcurrentHashMap<RingMember, RemoteVersionedState> ringMemberState = remoteStateCache.computeIfAbsent(partitionName,
            (key) -> {
                return new ConcurrentHashMap<>();
            });

        ringMemberState.merge(remoteRingMember, remoteVersionedState,
            (existing, updated) -> {
                return (updated.version > existing.version) ? updated : existing;
            });

    }

    public void bootstrap(Collection<RingMember> remoteRingMembers, VersionedPartitionName localVersionedPartitionName) throws Exception {

        ConcurrentHashMap<RingMember, RemoteVersionedState> ringMemberState = remoteStateCache.get(localVersionedPartitionName.getPartitionName());
        if (ringMemberState != null) {
            int inKetchup = 0;
            for (RingMember ringMember : remoteRingMembers) {
                RemoteVersionedState remoteRingMemberState = ringMemberState.get(ringMember);
                if (remoteRingMemberState == null) {
                    remoteRingMemberState = getRemoteState(ringMember, localVersionedPartitionName.getPartitionName());
                }
                if (remoteRingMemberState != null && PartitionState.KETCHUP == remoteRingMemberState.state) {
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
    public VersionedState getLocalState(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedState(PartitionState.ONLINE, 0, 0);
        }

        VersionedState versionedState = localStateCache.get(partitionName);
        if (versionedState != null) {
            return versionedState;
        }

        TimestampedValue rawState = systemWALStorage.getTimestampedValue(PartitionCreator.REGION_ONLINE_INDEX, null,
            walKey(rootRingMember, partitionName));
        versionedState = rawState == null ? null : VersionedState.fromBytes(rawState.getValue());
        if (versionedState == null || versionedState.stripeVersion != stripeVersions[partitionStripeFunction.stripe(partitionName)]) {
            return null;
        }
        return versionedState;
    }

    public RemoteVersionedState getRemoteState(RingMember ringMember, PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new RemoteVersionedState(PartitionState.ONLINE, 0);
        }

        //TODO consider wiring in remote state cache, for now it's racy
        TimestampedValue rawState = systemWALStorage.getTimestampedValue(PartitionCreator.REGION_ONLINE_INDEX, null, walKey(ringMember, partitionName));
        if (rawState == null) {
            return null;
        }
        VersionedState versionedState = VersionedState.fromBytes(rawState.getValue());
        return new RemoteVersionedState(versionedState.state, versionedState.version);
    }

    public VersionedState markAsKetchup(PartitionName partitionName) throws Exception {
        if (partitionName.isSystemPartition()) {
            return new VersionedState(PartitionState.ONLINE, 0, 0);
        }
        long partitionVersion = orderIdProvider.nextId();
        return set(rootRingMember, partitionName, new VersionedState(PartitionState.KETCHUP,
            partitionVersion,
            stripeVersions[partitionStripeFunction.stripe(partitionName)]));
    }

    public void markAsOnline(VersionedPartitionName versionedPartitionName) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return;
        }
        set(rootRingMember, versionedPartitionName.getPartitionName(), new VersionedState(PartitionState.ONLINE,
            versionedPartitionName.getPartitionVersion(),
            stripeVersions[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())]));
    }

    public VersionedState markForDisposal(VersionedPartitionName versionedPartitionName, RingMember ringMember) throws Exception {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return new VersionedState(PartitionState.ONLINE, 0, 0);
        }
        return set(ringMember, versionedPartitionName.getPartitionName(), new VersionedState(PartitionState.EXPUNGE,
            versionedPartitionName.getPartitionVersion(),
            stripeVersions[partitionStripeFunction.stripe(versionedPartitionName.getPartitionName())]));
    }

    public void streamLocalState(PartitionMemberStateStream stream) throws Exception {
        byte[] fromKey = walKey(rootRingMember, null);
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        systemWALStorage.rangeScan(PartitionCreator.REGION_ONLINE_INDEX, null, fromKey, null, toKey,
            (prefix, key, value, valueTimestamp, valueTombstone, valueVersion) -> {
                HeapFiler filer = new HeapFiler(key);
                UIO.readByte(filer, "serializationVersion");
                RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member"));
                PartitionName partitionName = PartitionName.fromBytes(UIO.readByteArray(filer, "partition"));
                VersionedState versionState = VersionedState.fromBytes(value);

                if (versionState.stripeVersion == stripeVersions[partitionStripeFunction.stripe(partitionName)]) {
                    return transactor.doWithOne(new VersionedPartitionName(partitionName, versionState.version),
                        versionState.state,
                        (versionedPartitionName, partitionState) -> stream.stream(partitionName, ringMember, versionState));
                } else {
                    return true;
                }

            });
    }

    private VersionedState set(RingMember ringMember,
        PartitionName partitionName,
        VersionedState versionedState) throws Exception {

        VersionedPartitionName versionedPartitionName = new VersionedPartitionName(partitionName, versionedState.version);
        VersionedState cachedState = localStateCache.get(partitionName);
        if (cachedState != null && cachedState.equals(versionedState)) {
            return versionedState;
        }

        return transactor.doWithAll(versionedPartitionName, versionedState.state,
            (currentVersionedPartitionName, state) -> {

                TimestampedValue rawState = systemWALStorage.getTimestampedValue(PartitionCreator.REGION_ONLINE_INDEX, null,
                    walKey(ringMember, partitionName));
                VersionedState commitableVersionState = null;
                VersionedState returnableState = null;
                long stripeVersion = stripeVersions[partitionStripeFunction.stripe(partitionName)];
                VersionedState currentVersionedState = rawState == null ? null : VersionedState.fromBytes(rawState.getValue());
                if (currentVersionedState == null || currentVersionedState.stripeVersion != stripeVersion) {
                    if (versionedState.state == PartitionState.KETCHUP) {
                        commitableVersionState = versionedState;
                        returnableState = versionedState;
                    }
                } else {
                    if (currentVersionedState.version == versionedState.version && isValidTransition(currentVersionedState, versionedState)) {
                        commitableVersionState = versionedState;
                        returnableState = versionedState;
                    } else {
                        returnableState = currentVersionedState;
                    }
                }
                if (commitableVersionState != null) {
                    byte[] versionedStateBytes = commitableVersionState.toBytes();
                    awaitNotify.notifyChange(partitionName, () -> {
                        long timestampAndVersion = orderIdProvider.nextId();
                        RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.REGION_ONLINE_INDEX, null,
                            (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                                walKey(ringMember, partitionName),
                                versionedStateBytes, timestampAndVersion, false, timestampAndVersion), walUpdated);
                        return !rowsChanged.isEmpty();
                    });
                    LOG.info("STATE {}: {} versionedPartitionName:{} was updated to {}",
                        rootRingMember, ringMember, versionedPartitionName, commitableVersionState);
                    if (rootRingMember.equals(ringMember)) {
                        takeCoordinator.stateChanged(amzaRingReader, versionedPartitionName, commitableVersionState.state);
                        takeCoordinator.awakeCya();
                    }
                }
                if (rootRingMember.equals(ringMember)) {
                    if (returnableState != null) {
                        localStateCache.put(partitionName, returnableState);
                    } else {
                        localStateCache.remove(partitionName);
                    }
                }
                return returnableState;
            });

    }

    private static boolean isValidTransition(VersionedState currentVersionedState, VersionedState versionedState) {
        return (currentVersionedState.state == PartitionState.KETCHUP && versionedState.state == PartitionState.ONLINE)
            || (currentVersionedState.state == PartitionState.ONLINE && versionedState.state == PartitionState.EXPUNGE);
    }

    public void expunged(List<VersionedPartitionName> composted) throws Exception {
        for (VersionedPartitionName compost : composted) {
            transactor.doWithAll(compost, PartitionState.EXPUNGE,
                (versionedPartitionName, partitionState) -> {
                    awaitNotify.notifyChange(compost.getPartitionName(), () -> {
                        long timestampAndVersion = orderIdProvider.nextId();
                        RowsChanged rowsChanged = systemWALStorage.update(PartitionCreator.REGION_ONLINE_INDEX, null,
                            (highwaters, scan) -> scan.row(orderIdProvider.nextId(),
                                walKey(rootRingMember, compost.getPartitionName()),
                                null, timestampAndVersion, true, timestampAndVersion),
                            walUpdated);
                        return !rowsChanged.isEmpty();
                    });
                    invalidateLocalStateCache(compost);
                    return null;
                });
        }
        takeCoordinator.expunged(composted);
    }

    private void invalidateLocalStateCache(VersionedPartitionName versionedPartitionName) {
        localStateCache.computeIfPresent(versionedPartitionName.getPartitionName(), (partitionName, versionedState) -> {
            if (versionedState.version == versionedPartitionName.getPartitionVersion()) {
                return null;
            } else {
                return versionedState;
            }
        });
    }

    public void awaitOnline(PartitionName partitionName, long timeoutMillis) throws Exception {
        awaitNotify.awaitChange(partitionName, () -> {
            VersionedState versionedState = getLocalState(partitionName);
            if (versionedState != null) {
                if (versionedState.state == PartitionState.EXPUNGE) {
                    throw new IllegalStateException("Partition is being expunged");
                } else if (versionedState.state == PartitionState.ONLINE) {
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

    public interface PartitionMemberStateStream {

        boolean stream(PartitionName partitionName, RingMember ringMember, VersionedState versionedState) throws Exception;
    }


}
