package com.jivesoftware.os.amza.client.aquarium;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.PartitionClientProvider;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.client.http.AmzaClientCommitable;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.aquarium.interfaces.LivelinessStorage;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaClientLivelinessStorage implements LivelinessStorage {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final PartitionProperties LIVELINESS_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0,
        false, Consistency.quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private final PartitionClientProvider partitionClientProvider;
    private final String serviceName;
    private final byte[] context;
    private final Member member;
    private final long startupVersion;
    private final int aquariumLivelinessStripes;

    private final long additionalSolverAfterNMillis;
    private final long abandonLeaderSolutionAfterNMillis;
    private final long abandonSolutionAfterNMillis;
    private final boolean useSolutionLog;

    public AmzaClientLivelinessStorage(PartitionClientProvider partitionClientProvider,
        String serviceName,
        byte[] context,
        Member member,
        long startupVersion,
        int aquariumLivelinessStripes,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        boolean useSolutionLog) {
        this.partitionClientProvider = partitionClientProvider;
        this.serviceName = serviceName;
        this.context = context;
        this.member = member;
        this.startupVersion = startupVersion;
        this.aquariumLivelinessStripes = aquariumLivelinessStripes;
        this.additionalSolverAfterNMillis = additionalSolverAfterNMillis;
        this.abandonLeaderSolutionAfterNMillis = abandonLeaderSolutionAfterNMillis;
        this.abandonSolutionAfterNMillis = abandonSolutionAfterNMillis;
        this.useSolutionLog = useSolutionLog;
    }

    @Override
    public boolean scan(Member rootMember, Member otherMember, LivelinessStream stream) throws Exception {
        byte[] from = livelinessKey(rootMember, otherMember);
        byte[] to = WALKey.prefixUpperExclusive(from);
        List<String> solutionLog = useSolutionLog ? Collections.synchronizedList(Lists.newArrayList()) : null;
        try {
            return livelinessClient().scanKeys(Consistency.quorum,
                false,
                keyRangeStream -> keyRangeStream.stream(null, from, null, to),
                (prefix, key, value, valueTimestamp, valueVersion) -> {
                    return streamLivelinessKey(key, (rootRingMember, isSelf, ackRingMember) -> {
                        if (!rootRingMember.equals(member) || valueVersion > startupVersion) {
                            return stream.stream(rootRingMember, isSelf, ackRingMember, valueTimestamp, valueVersion);
                        } else {
                            return true;
                        }
                    });
                },
                additionalSolverAfterNMillis,
                abandonLeaderSolutionAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.ofNullable(solutionLog));
        } catch (Throwable t) {
            if (solutionLog != null) {
                LOG.info("Failure during scan, solution log: {}", solutionLog);
            }
            throw t;
        }
    }

    @Override
    public boolean update(LivelinessUpdates updates) throws Exception {
        List<AmzaClientCommitable> commitables = Lists.newArrayList();
        boolean result = updates.updates((rootMember, otherMember, timestamp) -> {
            byte[] keyBytes = livelinessKey(rootMember, otherMember);
            commitables.add(new AmzaClientCommitable(keyBytes, new byte[0], timestamp));
            return true;
        });
        if (result && commitables.size() > 0) {
            List<String> solutionLog = useSolutionLog ? Collections.synchronizedList(Lists.newArrayList()) : null;
            try {
                livelinessClient().commit(Consistency.quorum,
                    null,
                    commitKeyValueStream -> {
                        for (AmzaClientCommitable commitable : commitables) {
                            if (!commitKeyValueStream.commit(commitable.key, commitable.value, commitable.timestamp, false)) {
                                return false;
                            }
                        }
                        return true;
                    },
                    additionalSolverAfterNMillis,
                    abandonSolutionAfterNMillis,
                    Optional.ofNullable(solutionLog));
            } catch (Throwable t) {
                if (solutionLog != null) {
                    LOG.info("Failure during update, solution log: {}", solutionLog);
                }
                throw t;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long get(Member rootMember, Member otherMember) throws Exception {
        long[] timestamp = { -1 };
        List<String> solutionLog = useSolutionLog ? Collections.synchronizedList(Lists.newArrayList()) : null;
        try {
            livelinessClient().get(Consistency.quorum,
                null,
                keyStream -> keyStream.stream(livelinessKey(rootMember, otherMember)),
                (prefix, key, value, valueTimestamp, valueVersion) -> {
                    timestamp[0] = valueTimestamp;
                    return true;
                },
                additionalSolverAfterNMillis,
                abandonLeaderSolutionAfterNMillis,
                abandonSolutionAfterNMillis,
                Optional.ofNullable(solutionLog));
        } catch (Throwable t) {
            if (solutionLog != null) {
                LOG.info("Failure during get, solution log: {}", solutionLog);
            }
            throw t;
        }
        return timestamp[0];
    }

    private byte[] livelinessKey(Member rootRingMember, Member ackRingMember) throws Exception {
        int contextSizeInBytes = context.length;
        if (rootRingMember != null && ackRingMember != null) {
            byte[] rootBytes = rootRingMember.getMember();
            byte[] ackBytes = ackRingMember.getMember();
            int rootSizeInBytes = 4 + rootBytes.length;
            int ackSizeInBytes = 4 + ackBytes.length;
            byte[] key = new byte[contextSizeInBytes + rootSizeInBytes + 1 + ackSizeInBytes];

            System.arraycopy(context, 0, key, 0, context.length);

            UIO.intBytes(rootBytes.length, key, contextSizeInBytes);
            System.arraycopy(rootBytes, 0, key, contextSizeInBytes + 4, rootBytes.length);

            key[contextSizeInBytes + rootSizeInBytes] = !rootRingMember.equals(ackRingMember) ? (byte) 1 : (byte) 0;

            UIO.intBytes(ackBytes.length, key, contextSizeInBytes + rootSizeInBytes + 1);
            System.arraycopy(ackBytes, 0, key, contextSizeInBytes + rootSizeInBytes + 1 + 4, ackBytes.length);

            return key;
        } else if (rootRingMember != null) {
            byte[] rootBytes = rootRingMember.getMember();
            int rootSizeInBytes = 4 + rootBytes.length;
            byte[] key = new byte[contextSizeInBytes + rootSizeInBytes];

            System.arraycopy(context, 0, key, 0, context.length);

            UIO.intBytes(rootBytes.length, key, contextSizeInBytes);
            System.arraycopy(rootBytes, 0, key, contextSizeInBytes + 4, rootBytes.length);

            return key;
        } else {
            byte[] key = new byte[contextSizeInBytes];
            System.arraycopy(context, 0, key, 0, context.length);
            return key;
        }
    }

    private boolean streamLivelinessKey(byte[] keyBytes, LivelinessKeyStream stream) throws Exception {
        int o = context.length;
        int rootRingMemberBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        byte[] rootRingMemberBytes = new byte[rootRingMemberBytesLength];
        System.arraycopy(keyBytes, o, rootRingMemberBytes, 0, rootRingMemberBytesLength);
        o += rootRingMemberBytesLength;
        boolean isOther = keyBytes[o] != 0;
        boolean isSelf = !isOther;
        o++;
        int ackRingMemberBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        byte[] ackRingMemberBytes = new byte[ackRingMemberBytesLength];
        System.arraycopy(keyBytes, o, ackRingMemberBytes, 0, ackRingMemberBytesLength);
        o += ackRingMemberBytesLength;

        return stream.stream(new Member(rootRingMemberBytes), isSelf, new Member(ackRingMemberBytes));
    }

    private interface LivelinessKeyStream {

        boolean stream(Member rootRingMember, boolean isSelf, Member ackRingMember) throws Exception;
    }

    private PartitionClient livelinessClient() throws Exception {
        int stripe = Math.abs(serviceName.hashCode() % aquariumLivelinessStripes);
        byte[] partitionBytes = ("aquarium-liveliness-" + stripe).getBytes(StandardCharsets.UTF_8);
        byte[] ringBytes = ("aquarium-" + stripe).getBytes(StandardCharsets.UTF_8);
        return partitionClientProvider.getPartition(new PartitionName(false, ringBytes, partitionBytes), 3, LIVELINESS_PROPERTIES);
    }
}
