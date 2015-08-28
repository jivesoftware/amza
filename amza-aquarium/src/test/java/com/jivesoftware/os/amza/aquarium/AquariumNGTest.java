package com.jivesoftware.os.amza.aquarium;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class AquariumNGTest {

    private static final Member MIN = new Member(UIO.intBytes(0));
    private static final Member MAX = new Member(UIO.intBytes(Integer.MAX_VALUE));
    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;
    private static final byte LIVELINESS = 2;

    @Test
    public void testTapTheGlass() throws Exception {

        NavigableMap<Key, TimestampedState<State>> rawState = new ConcurrentSkipListMap<>();
        NavigableMap<Key, TimestampedState<Void>> rawLiveliness = new ConcurrentSkipListMap<>();

        int aquariumNodeCount = 10;
        AtomicInteger ringSize = new AtomicInteger();
        AquariumNode[] nodes = new AquariumNode[aquariumNodeCount];
        for (int i = 0; i < aquariumNodeCount; i++) {
            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(i));
            Storage storage = new Storage(orderIdProvider, rawState, rawLiveliness);
            AtomicLong currentCount = new AtomicLong();
            TransitionQuorum ifYoureLuckyCurrentTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                if (currentCount.incrementAndGet() % 2 == 0) {
                    storage.setState(currentWaterline.getMember(), desiredVersion, desiredState, CURRENT);
                    return true;
                }
                return false;
            };
            AtomicLong desiredCount = new AtomicLong();
            TransitionQuorum ifYoureLuckyDesiredTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                if (desiredCount.incrementAndGet() % 2 == 0) {
                    storage.setState(currentWaterline.getMember(), desiredVersion, desiredState, DESIRED);
                    return true;
                }
                return false;
            };
            TransitionQuorum currentTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                storage.setState(currentWaterline.getMember(), desiredVersion, desiredState, CURRENT);
                return true;
            };
            TransitionQuorum desiredTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                storage.setState(currentWaterline.getMember(), desiredVersion, desiredState, DESIRED);
                return true;
            };
            nodes[i] = new AquariumNode(orderIdProvider, new Member(UIO.intBytes(i)), storage,
                storage, ifYoureLuckyCurrentTransitionQuorum, ifYoureLuckyDesiredTransitionQuorum,
                currentTransitionQuorum, desiredTransitionQuorum,
                ringSize);
        }

        ScheduledExecutorService service = Executors.newScheduledThreadPool(aquariumNodeCount);

        int running = 0;
        AquariumNode[] alive = new AquariumNode[aquariumNodeCount];
        Future[] aliveFutures = new Future[aquariumNodeCount];
        for (; running < 5; running++) {
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            ringSize.incrementAndGet();
            alive[running] = nodes[running];
        }
        String mode = "Start with 5 nodes...";
        awaitLeader(mode, alive, ringSize);

        mode = "Force each node to be a leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force all nodes to be a leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceDesiredState(State.leader);
        }
        awaitLeader(mode, alive, ringSize);

        mode = "Force each node to be a follower...";
        for (int i = 0; i < running; i++) {
            if (i < running - 1) {
                nodes[i].forceDesiredState(State.follower);
            }
            awaitLeader(mode, alive, ringSize);

        }

        mode = "Add 5 more nodes...";
        for (; running < nodes.length; running++) {
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 10, 10, TimeUnit.MILLISECONDS);
            ringSize.incrementAndGet();
            alive[running] = nodes[running];
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force each node to bootstrap...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.bootstrap);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force each node to inactive...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.inactive);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force each node to nominated...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.nominated);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force each node to demoted...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.demoted);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force each node to follower...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.follower);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force each node to leader...";
        for (int i = 0; i < running; i++) {
            nodes[i].forceCurrentState(State.leader);
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force last node to leader...";
        nodes[nodes.length - 1].forceDesiredState(State.leader);
        awaitLeader(mode, alive, ringSize);

        mode = "Expunge 5 nodes...";
        for (int i = 0; i < 5; i++) {
            nodes[i].forceDesiredState(State.expunged);
            nodes[i].awaitDesiredState(State.expunged);
            if (aliveFutures[i].cancel(true)) {
                alive[i] = null;
                ringSize.decrementAndGet();
            }
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Re-add 5 nodes...";
        for (int i = 0; i < 5; i++) {
            nodes[i].forceDesiredState(State.follower);
            aliveFutures[i] = service.scheduleWithFixedDelay(nodes[i], 10, 10, TimeUnit.MILLISECONDS);
            ringSize.incrementAndGet();
            alive[i] = nodes[i];
            nodes[i].awaitDesiredState(State.follower);
            awaitLeader(mode, alive, ringSize);
        }

//        for (int i = 0; i < 1000; i++) {
//            awaitLeader(rand, nodes);
//
//            AquariumNode node = nodes[rand.nextInt(nodes.length)];
//            node.forceLeader();
//        }
    }

    private void awaitLeader(String mode, AquariumNode[] nodes, AtomicInteger ringSize) throws Exception {

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 2_000_000) {

            int[] follower = { 0 };
            int[] leaders = { 0 };

            for (AquariumNode node : nodes) {
                if (node == null) {
                    continue;
                }
                node.readWaterlineTx.tx(node.ringSize.get(), node.member, (current, desired) -> {

                    ReadWaterline.Waterline currentWaterline = current.get();
                    ReadWaterline.Waterline desiredWaterline = desired.get();

                    if (currentWaterline != null && currentWaterline.isAtQuorum() && State.checkEquals(currentWaterline, desiredWaterline)) {
                        if (currentWaterline.getState() == State.leader) {
                            leaders[0]++;
                        }
                        if (currentWaterline.getState() == State.follower) {
                            follower[0]++;
                        }
                    }
                    System.out.println(UIO.bytesInt(node.member.getMemeber()) + " " + mode);
                    System.out.println("\tCurrent:" + currentWaterline);
                    //System.out.println("\t\t" + rawStorage.subMap(new Key(node.member, node.member, CURRENT), false, new Key(node.member, MAX, CURRENT),
                    // false));
                    System.out.println("\tDesired:" + desiredWaterline);
                    //System.out.println("\t\t" + rawStorage.subMap(new Key(node.member, node.member, DESIRED), false, new Key(node.member, MAX, DESIRED),
                    // false));
                    return true;
                });

            }

            if (leaders[0] == 1 && (leaders[0] + follower[0] == ringSize.get())) {
                System.out.println("<<<<<<<<<<<<<<<<<<<< Hooray >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                break;
            } else {
                System.out.println("----------------------------------------");
                Thread.sleep(100);
            }
        }

    }

    interface MakeLively {
        void hitWithPaddle(Member member);
    }

    class AquariumNode implements Runnable {

        private final Member member;
        private final OrderIdProvider orderIdProvider;
        private final ReadWaterlineTx readWaterlineTx;
        private final MakeLively makeLively;
        private final TransitionQuorum currentTransitionQuorum;
        private final TransitionQuorum desiredTransitionQuorum;
        private final AtomicInteger ringSize;
        private final Aquarium aquarium;

        public AquariumNode(OrderIdProvider orderIdProvider,
            Member member,
            ReadWaterlineTx readWaterlineTx,
            MakeLively makeLively,
            TransitionQuorum ifYoureLuckyCurrentTransitionQuorum,
            TransitionQuorum ifYoureLuckyDesiredTransitionQuorum,
            TransitionQuorum currentTransitionQuorum,
            TransitionQuorum desiredTransitionQuorum,
            AtomicInteger ringSize) {

            this.member = member;
            this.orderIdProvider = orderIdProvider;
            this.readWaterlineTx = readWaterlineTx;
            this.makeLively = makeLively;
            this.currentTransitionQuorum = currentTransitionQuorum;
            this.desiredTransitionQuorum = desiredTransitionQuorum;
            this.ringSize = ringSize;

            this.aquarium = new Aquarium(orderIdProvider, readWaterlineTx, ifYoureLuckyCurrentTransitionQuorum, ifYoureLuckyDesiredTransitionQuorum, member);

        }

        public void forceDesiredState(State state) throws Exception {
            readWaterlineTx.tx(ringSize.get(), member, (current, desired) -> {
                ReadWaterline.Waterline currentWaterline = current.get();
                if (currentWaterline != null) {
                    System.out.println("FORCING DESIRED " + state + ":" + member);
                    desiredTransitionQuorum.transition(currentWaterline, orderIdProvider.nextId(), state);
                }
                return true;
            });
        }

        public void forceCurrentState(State state) throws Exception {
            readWaterlineTx.tx(ringSize.get(), member, (current, desired) -> {
                ReadWaterline.Waterline currentWaterline = current.get();
                ReadWaterline.Waterline desiredWaterline = desired.get();
                if (currentWaterline != null && desiredWaterline != null) {
                    System.out.println("FORCING CURRENT " + state + ":" + member);
                    currentTransitionQuorum.transition(currentWaterline, desiredWaterline.getTimestamp(), state);
                }
                return true;
            });
        }

        public void awaitDesiredState(State state) throws Exception {
            boolean[] reachedDesired = { false };
            while (!reachedDesired[0]) {
                readWaterlineTx.tx(ringSize.get(), member, (current, desired) -> {
                    ReadWaterline.Waterline currentWaterline = current.get();
                    if (currentWaterline != null) {
                        ReadWaterline.Waterline desiredWaterline = desired.get();

                        reachedDesired[0] = currentWaterline.getState() == state &&
                            currentWaterline.isAtQuorum() &&
                            State.checkEquals(currentWaterline, desiredWaterline);
                    }
                    return true;
                });
                System.out.println(member + " awaitDesiredState " + state);
                Thread.sleep(100);
            }

        }

        @Override
        public void run() {
            try {
                aquarium.tapTheGlass(ringSize.get());
                makeLively.hitWithPaddle(member);
            } catch (Exception x) {
                x.printStackTrace();
            }
        }

    }

    class Storage implements ReadWaterlineTx, MakeLively {

        private final OrderIdProvider idProvider;
        private final NavigableMap<Key, TimestampedState<State>> stateStorage;
        private final NavigableMap<Key, TimestampedState<Void>> livelinessStorage;
        private final AtomicLong firstLivelinessTimestamp = new AtomicLong(-1);

        public Storage(OrderIdProvider idProvider,
            NavigableMap<Key, TimestampedState<State>> stateStorage,
            NavigableMap<Key, TimestampedState<Void>> livelinessStorage) {
            this.idProvider = idProvider;
            this.stateStorage = stateStorage;
            this.livelinessStorage = livelinessStorage;
        }

        void setState(Member member, long timestamp, State state, byte context) {

            Key key = new Key(member, member, context);

            stateStorage.compute(key, (k, myState) -> {
                long version = idProvider.nextId();
                if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                    return myState;
                } else {
                    return new TimestampedState<>(state, timestamp, version);
                }
            });
        }

        @Override
        public void hitWithPaddle(Member member) {
            setLiveliness(member, System.currentTimeMillis());
        }

        void setLiveliness(Member member, long timestamp) {

            Key key = new Key(member, member, LIVELINESS);

            TimestampedState<Void> timestampedState = livelinessStorage.compute(key, (k, myState) -> {
                long version = idProvider.nextId();
                if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                    return myState;
                } else {
                    return new TimestampedState<>(null, timestamp, version);
                }
            });
            firstLivelinessTimestamp.compareAndSet(-1, timestampedState.timestamp);
        }

        @Override
        public void tx(int ringSize, Member member, Tx tx) throws Exception {
            tx.tx(new ContextualReadWaterline(idProvider, stateStorage, livelinessStorage, ringSize, member, CURRENT, firstLivelinessTimestamp, 10_000),
                new ContextualReadWaterline(idProvider, stateStorage, livelinessStorage, ringSize, member, DESIRED, firstLivelinessTimestamp, 10_000));
        }

    }

    class ContextualReadWaterline implements ReadWaterline {

        private final OrderIdProvider idProvider;
        private final NavigableMap<Key, TimestampedState<State>> stateStorage;
        private final NavigableMap<Key, TimestampedState<Void>> livelinessStorage;
        private final int ringSize;
        private final Member member;
        private final byte context;
        private final AtomicLong firstLivelinessTimestamp;
        private final long deadAfterMillis;

        public ContextualReadWaterline(OrderIdProvider idProvider,
            NavigableMap<Key, TimestampedState<State>> stateStorage,
            NavigableMap<Key, TimestampedState<Void>> livelinessStorage, int ringSize,
            Member member,
            byte context,
            AtomicLong firstLivelinessTimestamp,
            long deadAfterMillis) {
            this.idProvider = idProvider;
            this.stateStorage = stateStorage;
            this.livelinessStorage = livelinessStorage;
            this.ringSize = ringSize;
            this.member = member;
            this.context = context;
            this.firstLivelinessTimestamp = firstLivelinessTimestamp;
            this.deadAfterMillis = deadAfterMillis;
        }

        @Override
        public ReadWaterline.Waterline get() {

            SortedMap<Key, TimestampedState<State>> subMap = stateStorage.subMap(new Key(member, member, context), new Key(member, MAX, context));
            TimestampedState<State> current = null;
            int acked = 0;
            for (Map.Entry<Key, TimestampedState<State>> e : subMap.entrySet()) {
                if (current == null && e.getKey().isSelf()) {
                    current = e.getValue();
                    acked++;
                } else if (current != null) {
                    TimestampedState<State> v = e.getValue();
                    if (v.state == current.state && v.timestamp == current.timestamp) {
                        acked++;
                    }
                }
            }
            if (current != null) {
                boolean atQuorum = acked > ringSize / 2;
                return new Waterline(member, current.state, current.timestamp, current.version, atQuorum, aliveUntilTimestamp());
            } else {
                return null;
            }
        }

        private long aliveUntilTimestamp() {
            if (deadAfterMillis <= 0) {
                return Long.MAX_VALUE;
            }

            SortedMap<Key, TimestampedState<Void>> subMap = livelinessStorage.subMap(new Key(member, member, LIVELINESS), new Key(member, MAX, LIVELINESS));
            TimestampedState<Void> current = null;
            long latestAck = -1;
            int acked = 0;
            for (Map.Entry<Key, TimestampedState<Void>> e : subMap.entrySet()) {
                if (current == null && e.getKey().isSelf()) {
                    current = e.getValue();
                    acked++;
                } else if (current != null) {
                    TimestampedState<Void> v = e.getValue();
                    if (v.state == current.state && v.timestamp >= (current.timestamp - deadAfterMillis)) {
                        latestAck = Math.max(latestAck, v.timestamp);
                        acked++;
                    }
                }
            }

            boolean atQuorum = acked > ringSize / 2;
            if (current != null && atQuorum) {
                return latestAck + deadAfterMillis;
            }
            return -1;
        }

        private long otherAliveUntilTimestamp(Member other) {
            if (deadAfterMillis <= 0) {
                return Long.MAX_VALUE;
            }

            long firstTimestamp = firstLivelinessTimestamp.get();
            if (firstTimestamp < 0) {
                return Long.MAX_VALUE;
            }

            TimestampedState<Void> timestampedState = livelinessStorage.get(new Key(member, other, LIVELINESS));
            if (timestampedState != null) {
                return timestampedState.timestamp + deadAfterMillis;
            }
            return firstTimestamp + deadAfterMillis;
        }

        @Override
        public void getOthers(ReadWaterline.StreamQuorumState stream) throws Exception {
            Member otherMember = null;
            TimestampedState<State> otherState = null;
            int acked = 0;
            for (Map.Entry<Key, TimestampedState<State>> e : stateStorage.subMap(new Key(MIN, MIN, context), new Key(MAX, MAX, context)).entrySet()) {
                if (otherMember != null && !otherMember.equals(e.getKey().a)) {
                    boolean otherHasQuorum = acked > ringSize / 2;
                    stream.stream(new Waterline(otherMember, otherState.state, otherState.timestamp, otherState.version, otherHasQuorum,
                        otherAliveUntilTimestamp(otherMember)));

                    otherMember = null;
                    otherState = null;
                    acked = 0;
                }

                if (otherMember == null && e.getKey().isSelf() && !member.equals(e.getKey().a)) {
                    otherMember = e.getKey().a;
                    otherState = e.getValue();
                }
                if (otherMember != null) {
                    TimestampedState<State> v = e.getValue();
                    if (v.state == otherState.state && v.timestamp == otherState.timestamp) {
                        acked++;
                    }
                }
            }

            if (otherMember != null) {
                boolean otherHasQuorum = acked > ringSize / 2;
                stream.stream(new Waterline(otherMember, otherState.state, otherState.timestamp, otherState.version, otherHasQuorum,
                    otherAliveUntilTimestamp(otherMember)));
            }

        }

        @Override
        public void acknowledgeOther() throws Exception {
            acknowledgeOther(stateStorage, context);
            acknowledgeOther(livelinessStorage, LIVELINESS); //TODO technically only done by one thread per node
        }

        private <S> void acknowledgeOther(NavigableMap<Key, TimestampedState<S>> storage, byte storageContext) throws Exception {
            Entry<Key, TimestampedState<S>> otherE = null;
            boolean coldstart = true;
            for (Map.Entry<Key, TimestampedState<S>> e : storage.subMap(new Key(MIN, MIN, storageContext), new Key(MAX, MAX, storageContext)).entrySet()) {
                if (otherE != null && !otherE.getKey().a.equals(e.getKey().a)) {
                    if (coldstart) {
                        TimestampedState<S> theirState = otherE.getValue();
                        storage.compute(new Key(otherE.getKey().a, member, otherE.getKey().context), (k, myState) -> {
                            if (myState != null && myState.timestamp == theirState.timestamp && myState.state == theirState.state) {
                                return myState;
                            } else {
                                return new TimestampedState<>(theirState.state, theirState.timestamp, idProvider.nextId());
                            }
                        });
                    }
                    otherE = null;
                    coldstart = true;
                }

                if (otherE == null && e.getKey().isSelf() && !member.equals(e.getKey().a)) {
                    otherE = e;
                }
                if (otherE != null
                    && member.equals(e.getKey().b)
                    && (e.getValue().state != otherE.getValue().state || e.getValue().timestamp != otherE.getValue().timestamp)) {
                    coldstart = false;

                    TimestampedState<S> theirState = otherE.getValue();
                    storage.compute(e.getKey(), (k, myState) -> {
                        if (myState != null && myState.timestamp == theirState.timestamp && myState.state == theirState.state) {
                            return myState;
                        } else {
                            return new TimestampedState<>(theirState.state, theirState.timestamp, idProvider.nextId());
                        }
                    });

                }
            }
            if (otherE != null && coldstart) {
                TimestampedState<S> theirState = otherE.getValue();
                storage.compute(new Key(otherE.getKey().a, member, otherE.getKey().context), (k, myState) -> {
                    if (myState != null && myState.timestamp == theirState.timestamp && myState.state == theirState.state) {
                        return myState;
                    } else {
                        return new TimestampedState<>(theirState.state, theirState.timestamp, idProvider.nextId());
                    }
                });
            }

        }
    }

    static class Key implements Comparable<Key> {

        final byte context; // current = 0, desired = 1;
        final boolean isSelf;
        final Member a;
        final Member b;

        public Key(Member a, Member b, byte context) {
            this.context = context;
            this.isSelf = (a == b);
            this.a = a;
            this.b = b;

        }

        public boolean isSelf() {
            return isSelf;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 53 * hash + Objects.hashCode(this.a);
            hash = 53 * hash + Objects.hashCode(this.b);
            hash = 53 * hash + this.context;
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Key other = (Key) obj;
            if (!Objects.equals(this.a, other.a)) {
                return false;
            }
            if (!Objects.equals(this.b, other.b)) {
                return false;
            }
            if (this.context != other.context) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Key{" + "a=" + a + ", b=" + b + ", context=" + context + '}';
        }

        @Override
        public int compareTo(Key o) {
            int c = Byte.compare(context, o.context);
            if (c != 0) {
                return c;
            }
            c = UnsignedBytes.lexicographicalComparator().compare(a.getMemeber(), o.a.getMemeber());
            if (c != 0) {
                return c;
            }
            c = -Boolean.compare(isSelf, o.isSelf);
            if (c != 0) {
                return c;
            }
            c = UnsignedBytes.lexicographicalComparator().compare(b.getMemeber(), o.b.getMemeber());
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
            return "TimestampedState{" +
                "state=" + state +
                ", timestamp=" + timestamp +
                ", version=" + version +
                '}';
        }

    }

}
