package com.jivesoftware.os.amza.aquarium;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class AquariumNGTest {

    private static final Member MIN = new Member(intBytes(0));
    private static final Member MAX = new Member(intBytes(Integer.MAX_VALUE));
    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;
    private static final byte LIVELINESS = 2;

    @Test
    public void testTapTheGlass() throws Exception {

        NavigableMap<Key, TimestampedState<State>> rawState = new ConcurrentSkipListMap<>();
        NavigableMap<Key, TimestampedState<Void>> rawLiveliness = new ConcurrentSkipListMap<>();

        AtomicInteger rawRingSize = new AtomicInteger();
        AtQuorum atQuorum = count -> count > rawRingSize.get() / 2;

        Map<Member, Integer> rawLifecycles = Maps.newConcurrentMap();

        int aquariumNodeCount = 10;
        AquariumNode[] nodes = new AquariumNode[aquariumNodeCount];
        int deadAfterMillis = 10_000;
        for (int i = 0; i < aquariumNodeCount; i++) {
            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(i));

            Member member = new Member(intBytes(i));

            MemberLifecycle<Integer> memberLifecycle = new MemberLifecycle<Integer>() {
                @Override
                public Integer get() throws Exception {
                    return rawLifecycles.computeIfAbsent(member, key -> 0);
                }

                @Override
                public Integer getOther(Member other) throws Exception {
                    return rawLifecycles.computeIfAbsent(other, key -> 0);
                }
            };

            LivelinessStorage livelinessStorage = new LivelinessStorage() {
                @Override
                public boolean scan(Member rootMember, Member otherMember, LivelinessStream stream) throws Exception {
                    Member minA = (rootMember == null) ? MIN : rootMember;
                    Member maxA = (rootMember == null) ? MAX : rootMember;
                    Member minB = (otherMember == null) ? minA : otherMember;
                    SortedMap<Key, TimestampedState<Void>> subMap = rawLiveliness.subMap(new Key(LIVELINESS, 0, minA, minB), new Key(LIVELINESS, 0, maxA, MAX));
                    for (Map.Entry<Key, TimestampedState<Void>> e : subMap.entrySet()) {
                        Key key = e.getKey();
                        TimestampedState<Void> v = e.getValue();
                        if (!stream.stream(key.a, key.isSelf, key.b, v.timestamp, v.version)) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public boolean update(LivelinessUpdates updates) throws Exception {
                    return updates.updates((rootMember, otherMember, timestamp) -> {
                        rawLiveliness.compute(new Key(LIVELINESS, 0, rootMember, otherMember), (key, myState) -> {
                            long version = orderIdProvider.nextId();
                            if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                                return myState;
                            } else {
                                return new TimestampedState<>(null, timestamp, version);
                            }
                        });
                        return true;
                    });
                }

                @Override
                public long get(Member rootMember, Member otherMember) throws Exception {
                    TimestampedState<Void> timestampedState = rawLiveliness.get(new Key(LIVELINESS, 0, rootMember, otherMember));
                    return timestampedState != null ? timestampedState.timestamp : -1;
                }
            };

            ContextualStateStorage currentStateStorage = new ContextualStateStorage(orderIdProvider, rawState, CURRENT);
            ContextualStateStorage desiredStateStorage = new ContextualStateStorage(orderIdProvider, rawState, DESIRED);

            AtomicLong firstLivelinessTimestamp = new AtomicLong(-1);
            Liveliness liveliness = new Liveliness(livelinessStorage, member, atQuorum, deadAfterMillis, firstLivelinessTimestamp);
            Storage storage = new Storage(currentStateStorage, desiredStateStorage, liveliness, memberLifecycle, atQuorum);
            AtomicLong currentCount = new AtomicLong();
            TransitionQuorum ifYoureLuckyCurrentTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                if (currentCount.incrementAndGet() % 2 == 0) {
                    storage.setCurrentState(currentWaterline.getMember(), desiredVersion, desiredState);
                    return true;
                }
                return false;
            };
            AtomicLong desiredCount = new AtomicLong();
            TransitionQuorum ifYoureLuckyDesiredTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                if (desiredCount.incrementAndGet() % 2 == 0) {
                    storage.setDesiredState(currentWaterline.getMember(), desiredVersion, desiredState);
                    return true;
                }
                return false;
            };
            TransitionQuorum currentTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                storage.setCurrentState(currentWaterline.getMember(), desiredVersion, desiredState);
                return true;
            };
            TransitionQuorum desiredTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                storage.setDesiredState(currentWaterline.getMember(), desiredVersion, desiredState);
                return true;
            };
            nodes[i] = new AquariumNode(orderIdProvider,
                member,
                storage,
                firstLivelinessTimestamp, liveliness,
                ifYoureLuckyCurrentTransitionQuorum,
                ifYoureLuckyDesiredTransitionQuorum,
                currentTransitionQuorum,
                desiredTransitionQuorum);
        }

        ScheduledExecutorService service = Executors.newScheduledThreadPool(aquariumNodeCount);

        int running = 0;
        AquariumNode[] alive = new AquariumNode[aquariumNodeCount];
        Future[] aliveFutures = new Future[aquariumNodeCount];
        for (; running < 5; running++) {
            rawLifecycles.compute(nodes[running].member, (member, value) -> (value != null) ? (value + 1) : 0);
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[running] = nodes[running];
        }
        String mode = "Start with 5 nodes...";
        awaitLeader(mode, alive, rawRingSize);

        mode = "Force each node to be a leader...";
        AquariumNode leader = null;
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each leader to follower and the back to leader...";
        for (int i = 0; i < running; i++) {

            leader.forceDesiredState(State.follower);
            leader.forceDesiredState(State.leader);
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force all nodes to be a leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
        }
        awaitLeader(mode, alive, rawRingSize);

        mode = "Force each node to be a follower...";
        for (int i = 0; i < running; i++) {
            if (i < running - 1) {
                nodes[i].forceDesiredState(State.follower);
            }
            leader = awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Prove immune to clock drift by moving leader...";
        for (int i = 0; i < 2; i++) {
            leader.clockDrift.set(20_000 * ((i % 2 == 0) ? - 1 : 1));
            AquariumNode newLeader = awaitLeader(mode, alive, rawRingSize);
            leader.clockDrift.set(0);
            leader = newLeader;
        }

        mode = "Prove immune to clock drift by moving followers...";
        for (int i = 0; i < running; i++) {
            for (int j = 0; j < running; j++) {
                if (nodes[j] != leader) {
                    nodes[j].clockDrift.set(-20_000 * ((i % 2 == 0) ? - 1 : 1));
                }
            }
            AquariumNode newLeader = awaitLeader(mode, alive, rawRingSize);
            leader.clockDrift.set(0);
            leader = newLeader;
            for (int j = 0; j < running; j++) {
                nodes[j].clockDrift.set(0);
            }
        }

        mode = "Add 5 more nodes...";
        for (; running < nodes.length; running++) {
            rawLifecycles.compute(nodes[running].member, (member, value) -> (value != null) ? (value + 1) : 0);
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[running] = nodes[running];
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to bootstrap...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.bootstrap);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to inactive...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.inactive);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to nominated...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.nominated);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to demoted...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.demoted);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to follower...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force each node to leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.leader);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Force last node to leader...";
        nodes[nodes.length - 1].forceDesiredState(State.leader);
        awaitLeader(mode, alive, rawRingSize);

        mode = "Expunge 5 nodes...";
        for (int i = 0; i < 5; i++) {
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].forceDesiredState(State.expunged);
            nodes[i].awaitDesiredState(State.expunged, nodes);
            if (aliveFutures[i].cancel(true)) {
                alive[i] = null;
                rawRingSize.decrementAndGet();
            }
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Re-add 5 nodes...";
        for (int i = 0; i < 5; i++) {
            nodes[i].clear();
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].forceDesiredState(State.follower);
            aliveFutures[i] = service.scheduleWithFixedDelay(nodes[i], 10, 10, TimeUnit.MILLISECONDS);
            rawRingSize.incrementAndGet();
            alive[i] = nodes[i];
            nodes[i].awaitDesiredState(State.follower, nodes);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Blow away node state...";
        for (int i = 0; i < running; i++) {
            nodes[i].clear();
            nodes[i].awaitCurrentState(State.leader, State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

        mode = "Change lifecycles...";
        for (int i = 0; i < running; i++) {
            rawLifecycles.compute(nodes[i].member, (member, value) -> (value != null) ? (value + 1) : 0);
            nodes[i].awaitCurrentState(State.leader, State.follower);
            awaitLeader(mode, alive, rawRingSize);
        }

    }

    private AquariumNode awaitLeader(String mode, AquariumNode[] nodes, AtomicInteger ringSize) throws Exception {

        AquariumNode leader = null;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2_000_000) {

            int follower = 0;
            int leaders = 0;
            for (AquariumNode node : nodes) {
                if (node == null) {
                    continue;
                }

                State state = node.aquarium.livelyEndState();
                if (state == State.follower) {
                    follower++;
                }
                if (state == State.leader) {
                    leader = node;
                    leaders++;
                }

                node.printState(mode);

            }

            if (leaders == 1 && (leaders + follower == ringSize.get())) {
                System.out.println("<<<<<<<<<<<<<<<<<<<< Hooray >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                return leader;
            } else {
                System.out.println("----------------------------------------");
                Thread.sleep(100);
            }
        }
        Assert.fail();
        return null;
    }

    class AquariumNode implements Runnable {

        private final Member member;
        private final OrderIdProvider orderIdProvider;
        private final Storage readWaterlineTx;
        private final AtomicLong firstLivelinessTimestamp;
        private final TransitionQuorum currentTransitionQuorum;
        private final TransitionQuorum desiredTransitionQuorum;
        private final Aquarium aquarium;
        public final AtomicLong clockDrift = new AtomicLong(0);
        private final CurrentTimeMillis currentTimeMillis = () -> System.currentTimeMillis() + clockDrift.get();

        public AquariumNode(OrderIdProvider orderIdProvider,
            Member member,
            Storage readWaterlineTx,
            AtomicLong firstLivelinessTimestamp,
            Liveliness liveliness,
            TransitionQuorum ifYoureLuckyCurrentTransitionQuorum,
            TransitionQuorum ifYoureLuckyDesiredTransitionQuorum,
            TransitionQuorum currentTransitionQuorum,
            TransitionQuorum desiredTransitionQuorum) {

            this.member = member;
            this.orderIdProvider = orderIdProvider;
            this.readWaterlineTx = readWaterlineTx;
            this.firstLivelinessTimestamp = firstLivelinessTimestamp;
            this.currentTransitionQuorum = currentTransitionQuorum;
            this.desiredTransitionQuorum = desiredTransitionQuorum;

            this.aquarium = new Aquarium(orderIdProvider,
                currentTimeMillis,
                readWaterlineTx,
                liveliness,
                ifYoureLuckyCurrentTransitionQuorum,
                ifYoureLuckyDesiredTransitionQuorum,
                member);
        }

        public void forceDesiredState(State state) throws Exception {
            readWaterlineTx.tx(member, (current, desired) -> {
                Waterline currentWaterline = current.get();
                if (currentWaterline == null) {
                    currentWaterline = new Waterline(member, State.bootstrap, orderIdProvider.nextId(), -1L, true, Long.MAX_VALUE);
                }
                System.out.println("FORCING DESIRED " + state + ":" + member);
                desiredTransitionQuorum.transition(currentWaterline, orderIdProvider.nextId(), state);
                return true;
            });
        }

        public void forceCurrentState(State state) throws Exception {
            readWaterlineTx.tx(member, (current, desired) -> {
                Waterline currentWaterline = current.get();
                if (currentWaterline == null) {
                    currentWaterline = new Waterline(member, State.bootstrap, orderIdProvider.nextId(), -1L, true, Long.MAX_VALUE);
                }
                Waterline desiredWaterline = desired.get();
                if (desiredWaterline != null) {
                    System.out.println("FORCING CURRENT " + state + ":" + member);
                    currentTransitionQuorum.transition(currentWaterline, desiredWaterline.getTimestamp(), state);
                }
                return true;
            });
        }

        public void awaitCurrentState(State... states) throws Exception {
            Set<State> acceptable = Sets.newHashSet(states);
            boolean[] reachedCurrent = {false};
            while (!reachedCurrent[0]) {
                readWaterlineTx.tx(member, (current, desired) -> {
                    Waterline currentWaterline = current.get();
                    if (currentWaterline != null) {
                        reachedCurrent[0] = acceptable.contains(currentWaterline.getState()) && currentWaterline.isAtQuorum();
                    }
                    return true;
                });
                System.out.println(member + " awaitCurrentState " + Arrays.toString(states));
                Thread.sleep(100);
            }
        }

        public void awaitDesiredState(State state, AquariumNode[] nodes) throws Exception {
            boolean[] reachedDesired = {false};
            while (!reachedDesired[0]) {
                Waterline[] currentWaterline = {null};
                readWaterlineTx.tx(member, (current, desired) -> {
                    currentWaterline[0] = current.get();
                    if (currentWaterline[0] != null) {
                        Waterline desiredWaterline = desired.get();

                        reachedDesired[0] = currentWaterline[0].getState() == state
                            && currentWaterline[0].isAtQuorum()
                            && State.checkEquals(currentTimeMillis, currentWaterline[0], desiredWaterline);
                    }
                    return true;
                });
                for (AquariumNode node : nodes) {
                    if (node == this) {
                        System.out.println("->");
                    }
                    if (node != null) {
                        node.printState("awaitDesiredState " + member + "." + state);
                    }
                }

                Thread.sleep(100);
            }
        }

        @Override
        public void run() {
            try {
                aquarium.feedTheFish();
                aquarium.tapTheGlass();
            } catch (Exception x) {
                x.printStackTrace();
            }
        }

        private void clear() {
            firstLivelinessTimestamp.set(-1);
            readWaterlineTx.clear(member);
        }

        private void printState(String mode) throws Exception {
            readWaterlineTx.tx(member, (current, desired) -> {
                Waterline currentWaterline = current.get();
                Waterline desiredWaterline = desired.get();
                System.out.println(bytesInt(member.getMember()) + " " + mode);
                System.out.println("\tCurrent:" + currentWaterline);
                System.out.println("\tDesired:" + desiredWaterline);
                return true;
            });
        }

    }

    class Storage implements ReadWaterlineTx {

        private final ContextualStateStorage currentStateStorage;
        private final ContextualStateStorage desiredStateStorage;
        private final Liveliness liveliness;
        private final MemberLifecycle<Integer> memberLifecycle;
        private final AtQuorum ringSize;

        public Storage(ContextualStateStorage currentStateStorage,
            ContextualStateStorage desiredStateStorage,
            Liveliness liveliness,
            MemberLifecycle<Integer> memberLifecycle,
            AtQuorum ringSize) {
            this.currentStateStorage = currentStateStorage;
            this.desiredStateStorage = desiredStateStorage;
            this.liveliness = liveliness;
            this.memberLifecycle = memberLifecycle;
            this.ringSize = ringSize;
        }

        void setCurrentState(Member member, long timestamp, State state) throws Exception {
            currentStateStorage.update(setLiveliness -> setLiveliness.set(member, member, memberLifecycle.get(), state, timestamp));
        }

        void setDesiredState(Member member, long timestamp, State state) throws Exception {
            desiredStateStorage.update(setLiveliness -> setLiveliness.set(member, member, memberLifecycle.get(), state, timestamp));
        }

        @Override
        public void tx(Member member, Tx tx) throws Exception {
            tx.tx(new ReadWaterline<>(currentStateStorage, liveliness, memberLifecycle, ringSize, member, Integer.class),
                new ReadWaterline<>(desiredStateStorage, liveliness, memberLifecycle, ringSize, member, Integer.class));
        }

        public void clear(Member rootMember) {
            currentStateStorage.clear(rootMember);
            desiredStateStorage.clear(rootMember);
        }

    }

    private static class ContextualStateStorage implements StateStorage<Integer> {

        private final OrderIdProvider orderIdProvider;
        private final NavigableMap<Key, TimestampedState<State>> stateStorage;
        private final byte context;

        public ContextualStateStorage(OrderIdProvider orderIdProvider,
            NavigableMap<Key, TimestampedState<State>> stateStorage,
            byte context) {
            this.orderIdProvider = orderIdProvider;
            this.stateStorage = stateStorage;
            this.context = context;
        }

        @Override
        public boolean scan(Member rootMember, Member otherMember, Integer lifecycle, StateStream<Integer> stream) throws Exception {
            Member minA = (rootMember == null) ? MIN : rootMember;
            Member maxA = (rootMember == null) ? MAX : rootMember;
            Member minB = (otherMember == null) ? minA : otherMember;
            int minLifecycle = (lifecycle != null) ? lifecycle : Integer.MAX_VALUE; // reversed
            int maxLifecycle = (lifecycle != null) ? lifecycle : Integer.MIN_VALUE; // reversed
            SortedMap<Key, TimestampedState<State>> subMap = stateStorage.subMap(new Key(context, minLifecycle, minA, minB),
                new Key(context, maxLifecycle, maxA, MAX));
            for (Map.Entry<Key, TimestampedState<State>> e : subMap.entrySet()) {
                Key key = e.getKey();
                TimestampedState<State> v = e.getValue();
                if (!stream.stream(key.a, key.isSelf, key.b, key.memberVersion, v.state, v.timestamp, v.version)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean update(StateUpdates<Integer> updates) throws Exception {
            return updates.updates((rootMember, otherMember, lifecycle, state, timestamp) -> {
                stateStorage.compute(new Key(context, lifecycle, rootMember, otherMember), (key, myState) -> {
                    long version = orderIdProvider.nextId();
                    if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                        return myState;
                    } else {
                        return new TimestampedState<>(state, timestamp, version);
                    }
                });
                return true;
            });
        }

        public void clear(Member rootMember) {
            SortedMap<Key, TimestampedState<State>> subMap = stateStorage.subMap(new Key(context, Integer.MAX_VALUE, rootMember, rootMember),
                new Key(context, Integer.MIN_VALUE, rootMember, MAX));
            subMap.clear();
        }
    }

    static class Key implements Comparable<Key> {

        final byte context; // current = 0, desired = 1;
        final int memberVersion;
        final boolean isSelf;
        final Member a;
        final Member b;

        public Key(byte context, int memberVersion, Member a, Member b) {
            this.context = context;
            this.memberVersion = memberVersion;
            this.isSelf = a.equals(b);
            this.a = a;
            this.b = b;

        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException("stop");
        }

        @Override
        public boolean equals(Object obj) {
            throw new UnsupportedOperationException("stop");
        }

        @Override
        public String toString() {
            return "Key{"
                + "context=" + context
                + ", memberVersion=" + memberVersion
                + ", isSelf=" + isSelf
                + ", a=" + a
                + ", b=" + b
                + '}';
        }

        @Override
        public int compareTo(Key o) {
            int c = Byte.compare(context, o.context);
            if (c != 0) {
                return c;
            }
            c = -Integer.compare(memberVersion, o.memberVersion);
            if (c != 0) {
                return c;
            }
            c = UnsignedBytes.lexicographicalComparator().compare(a.getMember(), o.a.getMember());
            if (c != 0) {
                return c;
            }
            c = -Boolean.compare(isSelf, o.isSelf);
            if (c != 0) {
                return c;
            }
            c = UnsignedBytes.lexicographicalComparator().compare(b.getMember(), o.b.getMember());
            if (c != 0) {
                return c;
            }
            return c;
        }

    }

    static class TimestampedState<S> {

        final S state;
        final long timestamp;
        final long version;

        public TimestampedState(S state, long timestamp, long version) {
            this.state = state;
            this.timestamp = timestamp;
            this.version = version;
        }

        @Override
        public String toString() {
            return "TimestampedState{"
                + "state=" + state
                + ", timestamp=" + timestamp
                + ", version=" + version
                + '}';
        }

    }

    static byte[] intBytes(int v) {
        return intBytes(v, new byte[4], 0);
    }

    static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

    static int bytesInt(byte[] _bytes) {
        return bytesInt(_bytes, 0);
    }

    static int bytesInt(byte[] bytes, int _offset) {
        int v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        return v;
    }
}
