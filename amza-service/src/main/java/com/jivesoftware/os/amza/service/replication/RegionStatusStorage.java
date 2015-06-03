package com.jivesoftware.os.amza.service.replication;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.storage.RegionProvider;
import com.jivesoftware.os.amza.shared.region.RegionName;
import com.jivesoftware.os.amza.shared.region.RegionTx;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.region.TxRegionStatus;
import com.jivesoftware.os.amza.shared.region.VersionedRegionName;
import com.jivesoftware.os.amza.shared.region.VersionedRegionTransactor;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
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
public class RegionStatusStorage implements TxRegionStatus {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final RingMember rootRingMember;
    private final RegionStripe systemRegionStripe;
    private final VersionedRegionTransactor transactor;

    public RegionStatusStorage(OrderIdProvider orderIdProvider,
        RingMember rootRingMember,
        RegionStripe systemRegionStripe) {

        this.orderIdProvider = orderIdProvider;
        this.rootRingMember = rootRingMember;
        this.systemRegionStripe = systemRegionStripe;
        this.transactor = new VersionedRegionTransactor(1024, 1024); // TODO expose to config?
    }

    WALKey walKey(RingMember member, RegionName regionName) throws IOException {
        HeapFiler filer = new HeapFiler();
        UIO.writeByte(filer, 0, "serializationVersion");
        UIO.writeByteArray(filer, member.toBytes(), "member");
        if (regionName != null) {
            UIO.writeByteArray(filer, regionName.toBytes(), "region");
        }
        return new WALKey(filer.getBytes());
    }

    @Override
    public <R> R tx(RegionName regionName, RegionTx<R> tx) throws Exception {
        if (regionName.isSystemRegion()) {
            return tx.tx(new VersionedRegionName(regionName, 0), Status.ONLINE);
        }

        WALValue rawStatus = systemRegionStripe.get(RegionProvider.REGION_ONLINE_INDEX.getRegionName(), walKey(rootRingMember, regionName));
        if (rawStatus == null) {
            return tx.tx(null, null);
        } else {
            VersionedStatus versionedStatus = VersionedStatus.fromBytes(rawStatus.getValue());
            return transactor.doWithOne(new VersionedRegionName(regionName, versionedStatus.version), versionedStatus.status, tx);
        }
    }

    public void elect(Collection<RingMember> ringMembers, Set<RingMember> membersUnreachable, VersionedRegionName versionedRegionName) throws Exception {
        if (versionedRegionName.getRegionName().isSystemRegion() || ringMembers.isEmpty()) {
            return;
        }
        for (RingMember ringMember : ringMembers) {
            WALKey key = walKey(ringMember, versionedRegionName.getRegionName());
            WALValue rawStatus = systemRegionStripe.get(RegionProvider.REGION_ONLINE_INDEX.getRegionName(), key);
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
        markAsOnline(versionedRegionName);
    }

    public VersionedStatus getStatus(RingMember ringMember, RegionName regionName) throws Exception {
        WALValue rawStatus = systemRegionStripe.get(RegionProvider.REGION_ONLINE_INDEX.getRegionName(), walKey(rootRingMember, regionName));
        if (rawStatus == null || rawStatus.getTombstoned()) {
            return null;
        }
        return VersionedStatus.fromBytes(rawStatus.getValue());
    }

    public VersionedRegionName markAsKetchup(RegionName regionName) throws Exception {
        if (regionName.isSystemRegion()) {
            return new VersionedRegionName(regionName, 0);
        }
        long regionVersion = orderIdProvider.nextId();
        VersionedStatus versionedStatus = set(rootRingMember, regionName, new VersionedStatus(Status.KETCHUP, regionVersion));
        return new VersionedRegionName(regionName, versionedStatus.version);
    }

    public void markAsOnline(VersionedRegionName versionedRegionName) throws Exception {
        if (versionedRegionName.getRegionName().isSystemRegion()) {
            return;
        }
        set(rootRingMember, versionedRegionName.getRegionName(), new VersionedStatus(Status.ONLINE, versionedRegionName.getRegionVersion()));
    }

    public void markForDisposal(VersionedRegionName versionedRegionName, RingMember ringMember) throws Exception {
        if (versionedRegionName.getRegionName().isSystemRegion()) {
            return;
        }
        set(ringMember, versionedRegionName.getRegionName(), new VersionedStatus(Status.EXPUNGE, versionedRegionName.getRegionVersion()));
    }

    public void streamLocalState(RegionMemberStatusStream stream) throws Exception {
        WALKey from = walKey(rootRingMember, null);
        WALKey to = from.prefixUpperExclusive();
        systemRegionStripe.rangeScan(RegionProvider.REGION_ONLINE_INDEX.getRegionName(), from, to, (rowTxId, key, value) -> {
            HeapFiler filer = new HeapFiler(key.getKey());
            UIO.readByte(filer, "serializationVersion");
            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(filer, "member"));
            RegionName regionName = RegionName.fromBytes(UIO.readByteArray(filer, "region"));

            VersionedStatus versionStatus = VersionedStatus.fromBytes(value.getValue());

            return transactor.doWithOne(new VersionedRegionName(regionName, versionStatus.version),
                versionStatus.status,
                (versionedRegionName, regionStatus) -> stream.stream(regionName, ringMember, versionStatus));
        });
    }

    private final Map<VersionedRegionName, VersionedStatus> statusCache = Maps.newConcurrentMap();

    private VersionedStatus set(RingMember ringMember, RegionName regionName, VersionedStatus versionedStatus) throws Exception {
        VersionedRegionName versionedRegionName = new VersionedRegionName(regionName, versionedStatus.version);
        VersionedStatus cachedStatus = statusCache.get(versionedRegionName);
        if (cachedStatus != null && cachedStatus.equals(versionedStatus)) {
            return versionedStatus;
        }

        return transactor.doWithAll(versionedRegionName, versionedStatus.status, (currentVersionedRegionName, status) -> {

            WALValue rawStatus = systemRegionStripe.get(RegionProvider.REGION_ONLINE_INDEX.getRegionName(), walKey(rootRingMember, regionName));
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
                systemRegionStripe.commit(RegionProvider.REGION_ONLINE_INDEX.getRegionName(),
                    Optional.absent(),
                    false,
                    (highwaters, scan) -> {
                        scan.row(orderIdProvider.nextId(),
                            walKey(ringMember, regionName),
                            new WALValue(versionedStatusBytes, orderIdProvider.nextId(), false));
                    });
                LOG.info("{}: {} versionedRegionName:{} was updated to {}", rootRingMember, ringMember, versionedRegionName, commitableVersionStatus);
                statusCache.put(currentVersionedRegionName, commitableVersionStatus);
            }
            return returnableStatus;
        });

    }

    private static boolean isValidTransition(VersionedStatus currentVersionedStatus, VersionedStatus versionedStatus) {
        return (currentVersionedStatus.status == Status.KETCHUP && versionedStatus.status == Status.ONLINE)
            || (currentVersionedStatus.status == Status.ONLINE && versionedStatus.status == Status.EXPUNGE);
    }

    public void expunged(List<VersionedRegionName> composted) throws Exception {
        for (VersionedRegionName compost : composted) {
            transactor.doWithAll(compost, Status.EXPUNGE, (versionedRegionName, regionStatus) -> {
                systemRegionStripe.commit(RegionProvider.REGION_ONLINE_INDEX.getRegionName(),
                    Optional.absent(),
                    false,
                    (highwaters, scan) -> {
                        scan.row(orderIdProvider.nextId(),
                            walKey(rootRingMember, compost.getRegionName()),
                            new WALValue(null, orderIdProvider.nextId(), true));
                    });
                statusCache.remove(compost);
                return null;
            });
        }
    }

    public interface RegionMemberStatusStream {

        boolean stream(RegionName regionName, RingMember ringMember, VersionedStatus versionedStatus) throws Exception;
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
    }
}
