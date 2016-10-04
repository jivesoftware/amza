package com.jivesoftware.os.amza.service.take;

import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.PingStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StreamingTakesConsumer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final BAInterner interner;

    public StreamingTakesConsumer(BAInterner interner) {
        this.interner = interner;
    }

    public void consume(DataInputStream dis, AvailableStream updatedPartitionsStream, PingStream pingStream) throws Exception {
        while (dis.read() == 1) {
            int partitionNameLength = dis.readInt();
            if (partitionNameLength == 0) {
                pingStream.ping();
                continue;
            }
            byte[] versionedPartitionNameBytes = new byte[partitionNameLength];
            dis.readFully(versionedPartitionNameBytes);
            long txId = dis.readLong();
            try {
                updatedPartitionsStream.available(VersionedPartitionName.fromBytes(versionedPartitionNameBytes, 0, interner), txId);
            } catch (PropertiesNotPresentException e) {
                LOG.warn(e.getMessage());
            } catch (Throwable t) {
                LOG.error("Encountered problem while streaming available rows", t);
            }
        }
    }

    public StreamingTakeConsumed consume(DataInputStream is, RowStream tookRowUpdates) throws Exception {
        Map<RingMember, Long> neighborsHighwaterMarks = new HashMap<>();
        long leadershipToken;
        long partitionVersion;
        boolean isOnline;
        long bytes = 0;

        try (DataInputStream dis = is) {
            leadershipToken = dis.readLong();
            partitionVersion = dis.readLong();
            isOnline = dis.readByte() == 1;
            while (dis.readByte() == 1) {
                byte[] ringMemberBytes = new byte[dis.readInt()];
                dis.readFully(ringMemberBytes);
                long highwaterMark = dis.readLong();
                neighborsHighwaterMarks.put(RingMember.fromBytes(ringMemberBytes, 0, ringMemberBytes.length, interner), highwaterMark);
                bytes += 1 + 4 + ringMemberBytes.length + 8;
            }
            while (dis.readByte() == 1) {
                long rowTxId = dis.readLong();
                RowType rowType = RowType.fromByte(dis.readByte());
                byte[] rowBytes = new byte[dis.readInt()];
                dis.readFully(rowBytes);
                bytes += 1 + 8 + 1 + 4 + rowBytes.length;
                if (rowType != null) {
                    if (!tookRowUpdates.row(-1, rowTxId, rowType, rowBytes)) {
                        break;
                    }
                }
            }
        }
        return new StreamingTakeConsumed(leadershipToken, partitionVersion, isOnline, neighborsHighwaterMarks, bytes);

    }

    public static class StreamingTakeConsumed {

        public final long leadershipToken;
        public final long partitionVersion;
        public final boolean isOnline;
        public final Map<RingMember, Long> neighborsHighwaterMarks;
        public final long bytes;

        public StreamingTakeConsumed(long leadershipToken, long partitionVersion, boolean isOnline, Map<RingMember, Long> neighborsHighwaterMarks, long bytes) {
            this.leadershipToken = leadershipToken;
            this.partitionVersion = partitionVersion;
            this.isOnline = isOnline;
            this.neighborsHighwaterMarks = neighborsHighwaterMarks;
            this.bytes = bytes;
        }
    }
}
