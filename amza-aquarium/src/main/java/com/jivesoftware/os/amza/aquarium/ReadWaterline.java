package com.jivesoftware.os.amza.aquarium;

import com.google.common.collect.Sets;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.lang.reflect.Array;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class ReadWaterline<T> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final StateStorage<T> stateStorage;
    private final Liveliness liveliness;
    private final MemberLifecycle<T> memberLifecycle;
    private final AtQuorum atQuorum;
    private final Member member;
    private final Class<T> lifecycleType;

    public ReadWaterline(StateStorage<T> stateStorage,
        Liveliness liveliness,
        MemberLifecycle<T> memberLifecycle,
        AtQuorum atQuorum,
        Member member,
        Class<T> lifecycleType) {
        this.stateStorage = stateStorage;
        this.liveliness = liveliness;
        this.memberLifecycle = memberLifecycle;
        this.atQuorum = atQuorum;
        this.member = member;
        this.lifecycleType = lifecycleType;
    }

    public Waterline get() throws Exception {
        TimestampedState[] current = new TimestampedState[1];
        Set<Member> acked = Sets.newHashSet();
        T lifecycle = memberLifecycle.get();
        if (lifecycle == null) {
            LOG.info("Null lifecycle for {}", member);
            return null;
        }
        stateStorage.scan(member, null, lifecycle, (rootRingMember, isSelf, ackRingMember, rootLifecycle, state, timestamp, version) -> {
            if (current[0] == null && isSelf) {
                current[0] = new TimestampedState(state, timestamp, version);
            }
            if (current[0] != null) {
                TimestampedState v = new TimestampedState(state, timestamp, version);
                if (v.state == current[0].state && v.timestamp == current[0].timestamp) {
                    acked.add(ackRingMember);
                }
            }
            return true;
        });
        if (current[0] != null) {
            return new Waterline(member,
                current[0].state,
                current[0].timestamp,
                current[0].version,
                atQuorum.is(acked.size()),
                liveliness.aliveUntilTimestamp());
        } else {
            return null;
        }
    }

    public void getOthers(StreamQuorumState stream) throws Exception {
        Member[] otherMember = new Member[1];
        TimestampedState[] otherState = new TimestampedState[1];
        @SuppressWarnings("unchecked")
        T[] otherLifecycle = (T[]) Array.newInstance(lifecycleType, 1);
        Set<Member> acked = Sets.newHashSet();
        stateStorage.scan(null, null, null, (rootMember, isSelf, ackMember, rootLifecycle, state, timestamp, version) -> {
            if (otherMember[0] != null && !otherMember[0].equals(rootMember)) {
                boolean otherHasQuorum = atQuorum.is(acked.size());
                stream.stream(new Waterline(otherMember[0],
                    otherState[0].state,
                    otherState[0].timestamp,
                    otherState[0].version,
                    otherHasQuorum,
                    liveliness.otherAliveUntilTimestamp(otherMember[0])));

                otherMember[0] = null;
                otherState[0] = null;
                acked.clear();
            }

            if (otherMember[0] == null && isSelf && !member.equals(rootMember)) {
                T lifecycle = memberLifecycle.getOther(rootMember);
                if (lifecycle != null) {
                    otherMember[0] = rootMember;
                    otherState[0] = new TimestampedState(state, timestamp, version);
                    otherLifecycle[0] = lifecycle;
                }
            }
            if (otherMember[0] != null) {
                TimestampedState v = new TimestampedState(state, timestamp, version);
                if (otherLifecycle[0].equals(rootLifecycle) && v.state == otherState[0].state && v.timestamp == otherState[0].timestamp) {
                    acked.add(ackMember);
                }
            }
            return true;
        });

        if (otherMember[0] != null) {
            boolean otherHasQuorum = atQuorum.is(acked.size());
            stream.stream(new Waterline(otherMember[0],
                otherState[0].state,
                otherState[0].timestamp,
                otherState[0].version,
                otherHasQuorum,
                liveliness.otherAliveUntilTimestamp(otherMember[0])));
        }
    }

    public void acknowledgeOther() throws Exception {
        stateStorage.update(setState -> {
            @SuppressWarnings("unchecked")
            StateEntry<T>[] otherE = (StateEntry<T>[]) new StateEntry[1];
            boolean[] coldstart = { true };

            //byte[] fromKey = stateKey(versionedPartitionName.getPartitionName(), context, versionedPartitionName.getPartitionVersion(), null, null);
            stateStorage.scan(null, null, null, (rootMember, isSelf, ackMember, lifecycle, state, timestamp, version) -> {
                if (otherE[0] != null && !otherE[0].rootMember.equals(rootMember)) {
                    if (coldstart[0]) {
                        setState.set(otherE[0].rootMember, member, otherE[0].lifecycle, otherE[0].state, otherE[0].timestamp);
                    }
                    otherE[0] = null;
                    coldstart[0] = true;
                }

                if (otherE[0] == null && isSelf && !member.equals(rootMember)) {
                    otherE[0] = new StateEntry<>(rootMember, lifecycle, state, timestamp);
                }
                if (otherE[0] != null && member.equals(ackMember)) {
                    coldstart[0] = false;
                    if (state != otherE[0].state || timestamp != otherE[0].timestamp) {
                        setState.set(otherE[0].rootMember, member, otherE[0].lifecycle, otherE[0].state, otherE[0].timestamp);
                    }
                }
                return true;
            });
            if (otherE[0] != null && coldstart[0]) {
                setState.set(otherE[0].rootMember, member, otherE[0].lifecycle, otherE[0].state, otherE[0].timestamp);
            }
            return true;
        });
    }

    private static class StateEntry<T> {

        private final Member rootMember;
        private final T lifecycle;
        private final State state;
        private final long timestamp;

        public StateEntry(Member rootMember,
            T lifecycle,
            State state,
            long timestamp) {
            this.rootMember = rootMember;
            this.lifecycle = lifecycle;
            this.state = state;
            this.timestamp = timestamp;
        }
    }

    private static class TimestampedState {

        private final State state;
        private final long timestamp;
        private final long version;

        public TimestampedState(State state, long timestamp, long version) {
            this.state = state;
            this.timestamp = timestamp;
            this.version = version;
        }

        @Override
        public String toString() {
            return "TimestampedState{" +
                "state=" + state +
                ", timestamp=" + timestamp +
                ", version=" + version +
                '}';
        }
    }

}
