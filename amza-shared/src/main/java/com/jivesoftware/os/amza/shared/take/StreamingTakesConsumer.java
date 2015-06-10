package com.jivesoftware.os.amza.shared.take;

import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.RowStream;
import com.jivesoftware.os.amza.shared.scan.RowType;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StreamingTakesConsumer {

    public StreamingTakeConsumed consume(InputStream bis, RowStream tookRowUpdates) throws Exception {
        Map<RingMember, Long> neighborsHighwaterMarks = new HashMap<>();
        long partitionVersion;
        boolean isOnline;
        long bytes = 0;
        try (DataInputStream dis = new DataInputStream(bis)) {
            partitionVersion = dis.readLong();
            isOnline = dis.readByte() == 1;
            while (dis.readByte() == 1) {
                byte[] ringMemberBytes = new byte[dis.readInt()];
                dis.readFully(ringMemberBytes);
                long highwaterMark = dis.readLong();
                neighborsHighwaterMarks.put(RingMember.fromBytes(ringMemberBytes), highwaterMark);
                bytes += 1 + 4 + ringMemberBytes.length + 8;
            }
            while (dis.readByte() == 1) {
                long rowTxId = dis.readLong();
                RowType rowType = RowType.fromByte(dis.readByte());
                byte[] rowBytes = new byte[dis.readInt()];
                dis.readFully(rowBytes);
                bytes += 1 + 8 + 1 + 4 + rowBytes.length;
                if (rowType != null) {
                    tookRowUpdates.row(-1, rowTxId, rowType, rowBytes);
                }
            }
        }
        return new StreamingTakeConsumed(partitionVersion, isOnline, neighborsHighwaterMarks, bytes);
    }

    public static class StreamingTakeConsumed {
        public final long partitionVersion;
        public final boolean isOnline;
        public final Map<RingMember, Long> neighborsHighwaterMarks;
        public final long bytes;

        public StreamingTakeConsumed(long partitionVersion, boolean isOnline, Map<RingMember, Long> neighborsHighwaterMarks, long bytes) {
            this.partitionVersion = partitionVersion;
            this.isOnline = isOnline;
            this.neighborsHighwaterMarks = neighborsHighwaterMarks;
            this.bytes = bytes;
        }
    }
}
