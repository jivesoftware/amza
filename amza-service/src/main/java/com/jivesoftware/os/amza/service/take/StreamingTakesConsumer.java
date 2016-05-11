package com.jivesoftware.os.amza.service.take;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.RowStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.service.PropertiesNotPresentException;
import com.jivesoftware.os.amza.service.take.AvailableRowsTaker.AvailableStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.DataInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jersey.repackaged.com.google.common.collect.Maps;

/**
 *
 */
public class StreamingTakesConsumer implements Runnable {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final BAInterner interner;
    private final long interruptBlockingReadsIfLingersForNMillis;
    private final ScheduledExecutorService hanguper;
    private final Map<Thread, Hangupable> hangupables = Maps.newConcurrentMap();

    public StreamingTakesConsumer(BAInterner interner, String name, long interruptBlockingReadsIfLingersForNMillis) {
        this.interner = interner;
        this.interruptBlockingReadsIfLingersForNMillis = interruptBlockingReadsIfLingersForNMillis;
        this.hanguper = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(name + "-hanguper-%d").build());
        this.hanguper.scheduleWithFixedDelay(this, interruptBlockingReadsIfLingersForNMillis, interruptBlockingReadsIfLingersForNMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        for (Hangupable hangupable : hangupables.values()) {
            try {
                hangupable.hangupIfNecessary();
            } catch (Exception x) {
                LOG.warn("Error while hanging up connection.", x);
            }
        }
    }

    class Hangupable {

        private final Thread thread;
        private volatile long touchTimestamp = System.currentTimeMillis();

        public Hangupable(Thread thread) {
            this.thread = thread;
        }

        public void touch() {
            touchTimestamp = System.currentTimeMillis();
        }

        public boolean hangupIfNecessary() {
            if (touchTimestamp + interruptBlockingReadsIfLingersForNMillis < System.currentTimeMillis()) {
                LOG.info("Hanging up connection for thread:{}. This should typically happen when you loose a server or a GC pause exceeds: {} ", thread,
                    interruptBlockingReadsIfLingersForNMillis);
                thread.interrupt();
                return true;
            }
            return false;
        }

    }

    public void consume(DataInputStream dis, AvailableStream updatedPartitionsStream) throws Exception {
        try {
            Hangupable hangupable = hangupables.computeIfAbsent(Thread.currentThread(), (t) -> new Hangupable(t));
            hangupable.touch();
            while (dis.read() == 1) {
                hangupable.touch();
                int partitionNameLength = dis.readInt();
                if (partitionNameLength == 0) {
                    // this is a ping
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
        } finally {
            hangupables.remove(Thread.currentThread());
        }
    }

    public StreamingTakeConsumed consume(DataInputStream is, RowStream tookRowUpdates) throws Exception {
        Map<RingMember, Long> neighborsHighwaterMarks = new HashMap<>();
        long leadershipToken;
        long partitionVersion;
        boolean isOnline;
        long bytes = 0;
        try {
            Hangupable hangupable = hangupables.computeIfAbsent(Thread.currentThread(), (t) -> new Hangupable(t));
            hangupable.touch();
            try (DataInputStream dis = is) {
                leadershipToken = dis.readLong();
                partitionVersion = dis.readLong();
                isOnline = dis.readByte() == 1;
                while (dis.readByte() == 1) {
                    hangupable.touch();
                    byte[] ringMemberBytes = new byte[dis.readInt()];
                    dis.readFully(ringMemberBytes);
                    long highwaterMark = dis.readLong();
                    neighborsHighwaterMarks.put(RingMember.fromBytes(ringMemberBytes, 0, ringMemberBytes.length, interner), highwaterMark);
                    bytes += 1 + 4 + ringMemberBytes.length + 8;
                }
                while (dis.readByte() == 1) {
                    hangupable.touch();
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
        } finally {
            hangupables.remove(Thread.currentThread());
        }
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
