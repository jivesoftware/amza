package com.jivesoftware.os.amza.transport.http.replication.endpoints;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.Partition;
import com.jivesoftware.os.amza.shared.PartitionProvider;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
public class AmzaClientService implements AmzaRestClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRingReader ringReader;
    private final AmzaRingWriter ringWriter;
    private final PartitionProvider partitionProvider;

    public AmzaClientService(AmzaRingReader ringReader, AmzaRingWriter ringWriter, PartitionProvider partitionProvider) {
        this.ringReader = ringReader;
        this.ringWriter = ringWriter;
        this.partitionProvider = partitionProvider;
    }

    @Override
    public RingTopology configPartition(PartitionName partitionName, PartitionProperties partitionProperties, int ringSize) throws
        Exception {
        byte[] ringNameBytes = partitionName.getRingName();
        ringWriter.ensureSubRing(ringNameBytes, ringSize);
        partitionProvider.setPropertiesIfAbsent(partitionName, partitionProperties);
        return ringReader.getRing(partitionName.getRingName());
    }

    @Override
    public void configPartition(RingTopology ring, IWriteable writeable) throws Exception {
        byte[] lengthBuffer = new byte[4];
        UIO.writeInt(writeable, ring.entries.size(), "ringSize", lengthBuffer);
        for (RingMemberAndHost entry : ring.entries) {
            UIO.writeByteArray(writeable, entry.ringMember.toBytes(), "ringMember", lengthBuffer);
            UIO.writeByteArray(writeable, entry.ringHost.toBytes(), "ringHost", lengthBuffer);
            UIO.writeByte(writeable, (byte) 0, "leader");
        }
    }

    @Override
    public void ensurePartition(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        long start = System.currentTimeMillis();
        partitionProvider.awaitOnline(partitionName, waitForLeaderElection);
        partitionProvider.awaitLeader(partitionName, Math.max(0, waitForLeaderElection - (System.currentTimeMillis() - start)));
    }

    @Override
    public RingLeader ring(PartitionName partitionName) throws Exception {
        return new RingLeader(ringReader.getRing(partitionName.getRingName()), null);
    }

    @Override
    public RingLeader ringLeader(PartitionName partitionName, long waitForLeaderElection) throws Exception {
        RingMember leader = partitionName.isSystemPartition() ? null : partitionProvider.awaitLeader(partitionName, waitForLeaderElection);
        return new RingLeader(ringReader.getRing(partitionName.getRingName()), leader);
    }

    @Override
    public void ring(RingLeader ringLeader, IWriteable writeable) throws IOException {
        byte[] lengthBuffer = new byte[4];
        UIO.writeInt(writeable, ringLeader.ringTopology.entries.size(), "ringSize", lengthBuffer);
        for (RingMemberAndHost entry : ringLeader.ringTopology.entries) {
            UIO.writeByteArray(writeable, entry.ringMember.toBytes(), "ringMember", lengthBuffer);
            UIO.writeByteArray(writeable, entry.ringHost.toBytes(), "ringHost", lengthBuffer);
            boolean isLeader = ringLeader.leader != null && Arrays.equals(entry.ringMember.toBytes(), ringLeader.leader.toBytes());
            UIO.writeByte(writeable, isLeader ? (byte) 1 : (byte) 0, "leader");
        }
    }

    @Override
    public StateMessageCause commit(PartitionName partitionName,
        Consistency consistency,
        boolean checkLeader,
        long partitionAwaitOnlineTimeoutMillis,
        IReadable read) throws Exception {
        StateMessageCause response = checkForReadyState(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis);
        if (response != null) {
            return response;
        }

        Partition partition = partitionProvider.getPartition(partitionName);
        byte[] intLongBuffer = new byte[8];
        byte[] prefix = UIO.readByteArray(read, "prefix", intLongBuffer);
        long timeoutInMillis = UIO.readLong(read, "timeoutInMillis", intLongBuffer);

        partition.commit(consistency, prefix, (highwaters, txKeyValueStream) -> {
            while (!UIO.readBoolean(read, "eos")) {
                if (!txKeyValueStream.row(UIO.readLong(read, "rowTxId", intLongBuffer),
                    UIO.readByteArray(read, "key", intLongBuffer),
                    UIO.readByteArray(read, "value", intLongBuffer),
                    UIO.readLong(read, "valueTimestamp", intLongBuffer),
                    UIO.readBoolean(read, "valueTombstoned"),
                    -1)) {
                    return false;
                }
            }
            return true;
        }, timeoutInMillis);
        return null;
    }

    @Override
    public StateMessageCause status(PartitionName partitionName, Consistency consistency, boolean checkLeader,
        long partitionAwaitOnlineTimeoutMillis) {
        return checkForReadyState(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis);
    }

    @Override
    public void get(PartitionName partitionName, Consistency consistency, IReadable in, IWriteable out) throws Exception {
        Partition partition = partitionProvider.getPartition(partitionName);
        byte[] intLongBuffer = new byte[8];
        byte[] prefix = UIO.readByteArray(in, "prefix", intLongBuffer);

        partition.get(consistency, prefix,
            (keyStream) -> {
                while (!UIO.readBoolean(in, "eos")) {
                    if (!keyStream.stream(UIO.readByteArray(in, "key", intLongBuffer))) {
                        return false;
                    }
                }
                return true;
            },
            (rowType, prefix1, key, value, timestamp, tombstoned, version) -> {
                UIO.writeByte(out, (byte) 0, "eos");
                UIO.writeByteArray(out, prefix1, "prefix", intLongBuffer);
                UIO.writeByteArray(out, key, "key", intLongBuffer);
                if (tombstoned) {
                    UIO.writeByteArray(out, null, "value", intLongBuffer);
                    UIO.writeLong(out, -1L, "timestamp");
                    UIO.writeByte(out, (byte) 1, "tombstoned");
                    UIO.writeLong(out, -1L, "version");
                } else {
                    UIO.writeByteArray(out, value, "value", intLongBuffer);
                    UIO.writeLong(out, timestamp, "timestamp");
                    UIO.writeByte(out, (byte) 0, "tombstoned");
                    UIO.writeLong(out, version, "version");
                }
                return true;
            });

        UIO.writeByte(out, (byte) 1, "eos");
    }

    @Override
    public void scan(PartitionName partitionName, IReadable in, IWriteable out) throws Exception {
        byte[] intLongBuffer = new byte[8];
        Partition partition = partitionProvider.getPartition(partitionName);
        byte[] fromPrefix = UIO.readByteArray(in, "fromPrefix", intLongBuffer);
        byte[] fromKey = UIO.readByteArray(in, "fromKey", intLongBuffer);
        byte[] toPrefix = UIO.readByteArray(in, "toPrefix", intLongBuffer);
        byte[] toKey = UIO.readByteArray(in, "toKey", intLongBuffer);

        partition.scan(fromPrefix, fromKey, toPrefix, toKey,
            (prefix, key, value, timestamp, version) -> {
                UIO.writeByte(out, (byte) 0, "eos");
                UIO.writeByteArray(out, prefix, "prefix", intLongBuffer);
                UIO.writeByteArray(out, key, "key", intLongBuffer);
                UIO.writeByteArray(out, value, "value", intLongBuffer);
                UIO.writeLong(out, timestamp, "timestampId");
                UIO.writeLong(out, version, "version");
                return true;
            });

        UIO.writeByte(out, (byte) 1, "eos");
    }

    @Override
    public void takeFromTransactionId(PartitionName partitionName, IReadable in, IWriteable out) throws Exception {
        byte[] intLongBuffer = new byte[8];
        long transactionId = UIO.readLong(in, "transactionId", intLongBuffer);
        Partition partition = partitionProvider.getPartition(partitionName);
        take(out, partition, false, null, transactionId, intLongBuffer);
    }

    @Override
    public void takePrefixFromTransactionId(PartitionName partitionName, IReadable in, IWriteable out) throws Exception {
        byte[] intLongBuffer = new byte[8];
        Partition partition = partitionProvider.getPartition(partitionName);
        byte[] prefix = UIO.readByteArray(in, "prefix", intLongBuffer);
        long txId = UIO.readLong(in, "txId", intLongBuffer);
        take(out, partition, true, prefix, txId, intLongBuffer);
    }

    private void take(IWriteable out,
        Partition partition,
        boolean usePrefix,
        byte[] prefix,
        long txId,
        byte[] lengthBuffer) throws Exception {

        RingMember ringMember = ringReader.getRingMember();
        UIO.writeByteArray(out, ringMember.toBytes(), "ringMember", lengthBuffer);
        Highwaters streamHighwater = (highwater) -> {
            UIO.writeByte(out, (byte) 0, "eos");
            UIO.writeByte(out, RowType.highwater.toByte(), "type");
            writeHighwaters(out, highwater, lengthBuffer);
        };
        TxKeyValueStream stream = (rowTxId, prefix1, key, value, timestamp, tombstoned, version) -> {
            UIO.writeByte(out, (byte) 0, "eos");
            UIO.writeByte(out, RowType.primary.toByte(), "type");
            UIO.writeLong(out, rowTxId, "rowTxId");
            UIO.writeByteArray(out, prefix1, "prefix", lengthBuffer);
            UIO.writeByteArray(out, key, "key", lengthBuffer);
            UIO.writeByteArray(out, value, "value", lengthBuffer);
            UIO.writeLong(out, timestamp, "timestamp");
            UIO.writeByte(out, tombstoned ? (byte) 1 : (byte) 0, "tombstoned");
            UIO.writeLong(out, version, "version");
            return true;
        };
        TakeResult takeResult;
        if (usePrefix) {
            takeResult = partition.takePrefixFromTransactionId(prefix, txId, streamHighwater, stream);
        } else {
            takeResult = partition.takeFromTransactionId(txId, streamHighwater, stream);
        }
        UIO.writeByte(out, (byte) 1, "eos");

        UIO.writeByteArray(out, takeResult.tookFrom.toBytes(), "ringMember", lengthBuffer);
        UIO.writeLong(out, takeResult.lastTxId, "lastTxId");
        writeHighwaters(out, takeResult.tookToEnd, lengthBuffer);
        UIO.writeByte(out, (byte) 1, "eos");
    }

    private void writeHighwaters(IWriteable out, WALHighwater highwater, byte[] lengthBuffer) throws IOException {
        UIO.writeInt(out, highwater.ringMemberHighwater.size(), "length", lengthBuffer);
        for (WALHighwater.RingMemberHighwater ringMemberHighwater : highwater.ringMemberHighwater) {
            UIO.writeByteArray(out, ringMemberHighwater.ringMember.toBytes(), "ringMember", lengthBuffer);
            UIO.writeLong(out, ringMemberHighwater.transactionId, "txId");
        }
    }

    private StateMessageCause checkForReadyState(PartitionName partitionName,
        Consistency consistency,
        boolean checkLeader,
        long partitionAwaitOnlineTimeoutMillis) {

        try {
            partitionProvider.awaitOnline(partitionName, partitionAwaitOnlineTimeoutMillis); //TODO config
        } catch (Exception e) {
            return new StateMessageCause(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis,
                State.failed_to_come_online,
                "Partition didn't come online withing the alloted time of " + partitionAwaitOnlineTimeoutMillis + "millis", e);
        }
        if (checkLeader && consistency.requiresLeader()) {
            try {
                RingMember leader = partitionProvider.awaitLeader(partitionName, 0);
                if (leader == null) {
                    return new StateMessageCause(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis,
                        State.lacks_leader, "Lacks required leader.", null);
                }
                if (!leader.equals(ringReader.getRingMember())) {
                    return new StateMessageCause(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis,
                        State.not_the_leader, "Leader has changed.", null);
                }
            } catch (Exception x) {
                Object[] vals = new Object[]{partitionName, consistency};
                LOG.warn("Failed while determining leader {} at {}. ", vals, x);
                return new StateMessageCause(partitionName, consistency, checkLeader, partitionAwaitOnlineTimeoutMillis,
                    State.error, "Failed while determining leader: " + Arrays.toString(vals), x);
            }
        }
        return null;
    }

}
