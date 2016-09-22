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
import com.jivesoftware.os.aquarium.State;
import com.jivesoftware.os.aquarium.interfaces.StateStorage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaClientStateStorage implements StateStorage<Long> {

    private static final PartitionProperties STATE_PROPERTIES = new PartitionProperties(Durability.fsync_async,
        0, 0, 0, 0, TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(10), 0, 0,
        false, Consistency.quorum, true, true, false, RowType.primary, "lab", 8, null, -1, -1);

    private final PartitionClientProvider partitionClientProvider;
    private final String serviceName;
    private final byte[] context;
    private final int aquariumStateStripes;

    private final long additionalSolverAfterNMillis;
    private final long abandonLeaderSolutionAfterNMillis;
    private final long abandonSolutionAfterNMillis;

    public AmzaClientStateStorage(PartitionClientProvider partitionClientProvider,
        String serviceName,
        byte[] context,
        int aquariumStateStripes,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis, long abandonSolutionAfterNMillis) {
        this.partitionClientProvider = partitionClientProvider;
        this.serviceName = serviceName;
        this.context = context;
        this.aquariumStateStripes = aquariumStateStripes;
        this.additionalSolverAfterNMillis = additionalSolverAfterNMillis;
        this.abandonLeaderSolutionAfterNMillis = abandonLeaderSolutionAfterNMillis;
        this.abandonSolutionAfterNMillis = abandonSolutionAfterNMillis;
    }

    @Override
    public boolean scan(Member rootMember, Member otherMember, Long lifecycle, StateStream<Long> stream) throws Exception {
        byte[] fromKey = stateKey(rootMember, lifecycle, otherMember);
        return stateClient().scan(Consistency.quorum, false,
            keyRangeStream -> keyRangeStream.stream(null, fromKey, null, WALKey.prefixUpperExclusive(fromKey)),
            (prefix, key, value, timestamp, version) -> {
                return streamStateKey(key,
                    (rootRingMember, partitionVersion, isSelf, ackRingMember) -> {
                        State state = State.fromSerializedForm(value[0]);
                        return stream.stream(rootRingMember, isSelf, ackRingMember, partitionVersion, state, timestamp, version);
                    });
            },
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis,
            Optional.empty());
    }

    @Override
    public boolean update(StateUpdates<Long> updates) throws Exception {
        List<AmzaClientCommitable> commitables = Lists.newArrayList();
        boolean result = updates.updates(
            (rootMember, otherMember, lifecycle, state, timestamp) -> {
                byte[] keyBytes = stateKey(rootMember, lifecycle, otherMember);
                byte[] valueBytes = { state.getSerializedForm() };
                commitables.add(new AmzaClientCommitable(keyBytes, valueBytes, timestamp));
                return true;
            });
        if (result && commitables.size() > 0) {
            stateClient().commit(Consistency.quorum,
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
                Optional.empty());
            return true;
        } else {
            return false;
        }
    }

    private PartitionClient stateClient() throws Exception {
        int stripe = Math.abs(serviceName.hashCode() % aquariumStateStripes);
        byte[] partitionBytes = ("aquarium-state-" + stripe).getBytes(StandardCharsets.UTF_8);
        byte[] ringBytes = ("aquarium-" + stripe).getBytes(StandardCharsets.UTF_8);
        return partitionClientProvider.getPartition(new PartitionName(false, ringBytes, partitionBytes), 3, STATE_PROPERTIES);
    }

    private byte[] stateKey(Member rootRingMember, Long lifecycle, Member ackRingMember) throws Exception {
        int contextSizeInBytes = context.length;
        if (rootRingMember != null && ackRingMember != null) {
            byte[] rootBytes = rootRingMember.getMember();
            byte[] ackBytes = ackRingMember.getMember();
            int rootSizeInBytes = 4 + rootBytes.length;
            int ackSizeInBytes = 4 + ackBytes.length;
            byte[] key = new byte[contextSizeInBytes + rootSizeInBytes + 8 + 1 + ackSizeInBytes];

            System.arraycopy(context, 0, key, 0, context.length);

            UIO.intBytes(rootBytes.length, key, contextSizeInBytes);
            System.arraycopy(rootBytes, 0, key, contextSizeInBytes + 4, rootBytes.length);

            UIO.longBytes(lifecycle, key, contextSizeInBytes + rootSizeInBytes);

            key[contextSizeInBytes + rootSizeInBytes + 8] = !rootRingMember.equals(ackRingMember) ? (byte) 1 : (byte) 0;

            UIO.intBytes(ackBytes.length, key, contextSizeInBytes + rootSizeInBytes + 8 + 1);
            System.arraycopy(ackBytes, 0, key, contextSizeInBytes + rootSizeInBytes + 8 + 1 + 4, ackBytes.length);

            return key;
        } else if (rootRingMember != null) {
            byte[] rootBytes = rootRingMember.getMember();
            int rootSizeInBytes = 4 + rootBytes.length;
            byte[] key = new byte[contextSizeInBytes + rootSizeInBytes + 8];

            System.arraycopy(context, 0, key, 0, context.length);

            UIO.intBytes(rootBytes.length, key, contextSizeInBytes);
            System.arraycopy(rootBytes, 0, key, contextSizeInBytes + 4, rootBytes.length);

            UIO.longBytes(lifecycle, key, contextSizeInBytes + rootSizeInBytes);

            return key;
        } else {
            byte[] key = new byte[contextSizeInBytes];
            System.arraycopy(context, 0, key, 0, context.length);
            return key;
        }
    }

    private boolean streamStateKey(byte[] keyBytes, StateKeyStream stream) throws Exception {
        int o = context.length;
        int rootRingMemberBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        byte[] rootRingMemberBytes = new byte[rootRingMemberBytesLength];
        System.arraycopy(keyBytes, o, rootRingMemberBytes, 0, rootRingMemberBytesLength);
        o += rootRingMemberBytesLength;
        long lifecycle = UIO.bytesLong(keyBytes, o);
        o += 8;
        boolean isOther = keyBytes[o] != 0;
        boolean isSelf = !isOther;
        o++;
        int ackRingMemberBytesLength = UIO.bytesInt(keyBytes, o);
        o += 4;
        byte[] ackRingMemberBytes = new byte[ackRingMemberBytesLength];
        System.arraycopy(keyBytes, o, ackRingMemberBytes, 0, ackRingMemberBytesLength);
        o += ackRingMemberBytesLength;

        return stream.stream(new Member(rootRingMemberBytes),
            lifecycle,
            isSelf,
            new Member(ackRingMemberBytes));
    }

    private interface StateKeyStream {

        boolean stream(Member rootRingMember,
            long partitionVersion,
            boolean isSelf,
            Member ackRingMember) throws Exception;
    }

}
