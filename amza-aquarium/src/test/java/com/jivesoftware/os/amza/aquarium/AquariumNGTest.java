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
import java.util.Random;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class AquariumNGTest {

    private static final Member MIN = new Member(UIO.intBytes(0));
    private static final Member MAX = new Member(UIO.intBytes(Integer.MAX_VALUE));
    private static final byte CURRENT = 0;
    private static final byte DESIRED = 1;

    @Test
    public void testTapTheGlass() throws Exception {

        NavigableMap<Key, TimestampedState> rawStorage = new ConcurrentSkipListMap<>();

        int aquariumNodeCount = 10;
        AtomicInteger ringSize = new AtomicInteger();
        AquariumNode[] nodes = new AquariumNode[aquariumNodeCount];
        for (int i = 0; i < aquariumNodeCount; i++) {
            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(i));
            Storage storage = new Storage(orderIdProvider, rawStorage);
            TransitionQuorum currentTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                storage.set(currentWaterline.getMember(), desiredVersion, desiredState, CURRENT);
                return true;
            };
            TransitionQuorum desiredTransitionQuorum = (currentWaterline, desiredVersion, desiredState) -> {
                storage.set(currentWaterline.getMember(), desiredVersion, desiredState, DESIRED);
                return true;
            };
            nodes[i] = new AquariumNode(orderIdProvider, i, storage, currentTransitionQuorum, desiredTransitionQuorum, ringSize);
        }

        ScheduledExecutorService service = Executors.newScheduledThreadPool(aquariumNodeCount);

        int running = 0;
        AquariumNode[] alive = new AquariumNode[aquariumNodeCount];
        Future[] aliveFutures = new Future[aquariumNodeCount];
        for (; running < 5; running++) {
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 100, 100, TimeUnit.MILLISECONDS);
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
            aliveFutures[running] = service.scheduleWithFixedDelay(nodes[running], 100, 100, TimeUnit.MILLISECONDS);
            ringSize.incrementAndGet();
            alive[running] = nodes[running];
            awaitLeader(mode, alive, ringSize);
        }

        mode = "Force last node to leader...";
        nodes[nodes.length - 1].forceDesiredState(State.leader);
        awaitLeader(mode, alive, ringSize);

        mode = "Expundge 5 nodes...";
        for (int i = 0; i < 5; i++) {
            nodes[i].forceDesiredState(State.expunged);
            nodes[i].awaitDesiredState(State.expunged);
            if (aliveFutures[i].cancel(true)) {
                alive[i] = null;
                ringSize.decrementAndGet();
            }
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

            int[] follower = {0};
            int[] leaders = {0};

            for (AquariumNode node : nodes) {
                if (node == null) {
                    continue;
                }
                node.readWaterlineTx.tx(node.ringSize.get(), node.member, (current, desired) -> {

                    ReadWaterline.Waterline currentWaterline = current.get();
                    ReadWaterline.Waterline desiredWaterline = desired.get();

                    if (currentWaterline != null) {
                        if (currentWaterline.isHasQuorum() && currentWaterline.getState() == State.leader && currentWaterline.equals(desiredWaterline)) {
                            leaders[0]++;
                        }

                        if (currentWaterline.isHasQuorum() && currentWaterline.getState() == State.follower && currentWaterline.equals(desiredWaterline)) {
                            follower[0]++;
                        }
                    }
                    System.out.println(UIO.bytesInt(node.member.getMemeber()) + " " + mode);
                    System.out.println("\tCurrent:" + currentWaterline);
                    //System.out.println("\t\t" + rawStorage.subMap(new Key(node.member, node.member, CURRENT), false, new Key(node.member, MAX, CURRENT), false));
                    System.out.println("\tDesired:" + desiredWaterline);
                    //System.out.println("\t\t" + rawStorage.subMap(new Key(node.member, node.member, DESIRED), false, new Key(node.member, MAX, DESIRED), false));
                    return true;
                });

            }

            if (leaders[0] == 1 && (leaders[0] + follower[0] == ringSize.get())) {
                System.out.println("<<<<<<<<<<<<<<<<<<<< Hooray >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                break;
            } else {
                System.out.println("----------------------------------------");
                Thread.sleep(1000);
            }
        }

    }

    class AquariumNode implements Runnable {

        private final Random rand = new Random();
        private final Member member;
        private final OrderIdProvider orderIdProvider;
        private final ReadWaterlineTx readWaterlineTx;
        private final TransitionQuorum currentTransitionQuorum;
        private final TransitionQuorum desiredTransitionQuorum;
        private final AtomicInteger ringSize;

        private final Aquarium aquarium;

        public AquariumNode(OrderIdProvider orderIdProvider,
            int id,
            ReadWaterlineTx readWaterlineTx,
            TransitionQuorum currentTransitionQuorum,
            TransitionQuorum desiredTransitionQuorum,
            AtomicInteger ringSize) {

            this.orderIdProvider = orderIdProvider;
            this.readWaterlineTx = readWaterlineTx;
            this.currentTransitionQuorum = currentTransitionQuorum;
            this.desiredTransitionQuorum = desiredTransitionQuorum;
            this.ringSize = ringSize;

            member = new Member(UIO.intBytes(id));
            aquarium = new Aquarium(orderIdProvider, readWaterlineTx, currentTransitionQuorum, desiredTransitionQuorum, member);

        }

        public void forceDesiredState(State state) throws Exception {
            readWaterlineTx.tx(ringSize.get(), member, (ReadWaterline current, ReadWaterline desired) -> {
                ReadWaterline.Waterline currentWaterline = current.get();
                if (currentWaterline != null) {
                    System.out.println("FORCING " + state + ":" + member);
                    desiredTransitionQuorum.transition(currentWaterline, orderIdProvider.nextId(), state);
                }
                return true;
            });

        }

        public void awaitDesiredState(State state) throws Exception {
            boolean[] reachedDesired = {false};
            while (!reachedDesired[0]) {
                readWaterlineTx.tx(ringSize.get(), member, (ReadWaterline current, ReadWaterline desired) -> {
                    ReadWaterline.Waterline currentWaterline = current.get();
                    if (currentWaterline != null) {
                        ReadWaterline.Waterline desiredWaterline = desired.get();

                        reachedDesired[0] = currentWaterline.getState() == state && currentWaterline.isHasQuorum() && currentWaterline.equals(desiredWaterline);
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

//                if (rand.nextInt(100) > 99) {
//                    forceLeader();
//                }
                aquarium.tapTheGlass(ringSize.get());
            } catch (Exception x) {
                x.printStackTrace();
            }
        }

    }

    class Storage implements ReadWaterlineTx {

        private final OrderIdProvider idProvider;
        private final NavigableMap<Key, TimestampedState> storage;

        public Storage(OrderIdProvider idProvider, NavigableMap<Key, TimestampedState> storage) {
            this.idProvider = idProvider;
            this.storage = storage;
        }

        void set(Member member, long timestamp, State state, byte context) {

            Key key = new Key(member, member, context);

            storage.compute(key, (k, myState) -> {
                long version = idProvider.nextId();
                if (myState != null && (myState.timestamp > timestamp || (myState.timestamp == timestamp && myState.version > version))) {
                    return myState;
                } else {
                    return new TimestampedState(state, timestamp, version);
                }
            });
        }

        @Override
        public void tx(int ringSize, Member member, Tx tx) throws Exception {
            tx.tx(new ConextualReadWaterline(ringSize, member, CURRENT), new ConextualReadWaterline(ringSize, member, DESIRED));
        }

        class ConextualReadWaterline implements ReadWaterline {

            private final int ringSize;
            private final Member member;
            private final byte context;

            public ConextualReadWaterline(int ringSize, Member member, byte context) {
                this.ringSize = ringSize;
                this.member = member;
                this.context = context;
            }

            @Override
            public ReadWaterline.Waterline get() {

                SortedMap<Key, TimestampedState> subMap = storage.subMap(new Key(member, member, context), new Key(member, MAX, context));
                TimestampedState current = null;
                int acked = 0;
                for (Map.Entry<Key, TimestampedState> e : subMap.entrySet()) {
                    if (current == null && e.getKey().isSelf()) {
                        current = e.getValue();
                    }
                    if (current != null) {
                        TimestampedState v = e.getValue();
                        if (v.state == current.state && v.timestamp == current.timestamp) {
                            acked++;
                        }
                    }
                }
                if (current != null) {
                    boolean hasQuorum = acked > ringSize / 2;
                    return new Waterline(member, current.state, current.timestamp, hasQuorum);
                } else {
                    return null;
                }
            }

            @Override
            public void getOthers(ReadWaterline.StreamQuorumState stream) throws Exception {
                Member otherMember = null;
                TimestampedState otherState = null;
                int acked = 0;
                for (Map.Entry<Key, TimestampedState> e : storage.subMap(new Key(MIN, MIN, context), new Key(MAX, MAX, context)).entrySet()) {
                    if (otherMember != null && !otherMember.equals(e.getKey().a)) {

                        boolean otherHasQuorum = acked > ringSize / 2;
                        stream.stream(new Waterline(otherMember, otherState.state, otherState.timestamp, otherHasQuorum));

                        otherMember = null;
                        otherState = null;
                        acked = 0;
                    }

                    if (otherMember == null && e.getKey().isSelf() && !member.equals(e.getKey().a)) {
                        otherMember = e.getKey().a;
                        otherState = e.getValue();
                    }
                    if (otherMember != null) {
                        TimestampedState v = e.getValue();
                        if (v.state == otherState.state && v.timestamp == otherState.timestamp) {
                            acked++;
                        }
                    }
                }

                if (otherMember != null) {
                    boolean otherHasQuorum = acked > ringSize / 2;
                    stream.stream(new Waterline(otherMember, otherState.state, otherState.timestamp, otherHasQuorum));
                }

            }

            @Override
            public void acknowledgeOther() throws Exception {
                Entry<Key, TimestampedState> otherE = null;
                boolean coldstart = true;
                for (Map.Entry<Key, TimestampedState> e : storage.subMap(new Key(MIN, MIN, context), new Key(MAX, MAX, context)).entrySet()) {
                    if (otherE != null && !otherE.getKey().a.equals(e.getKey().a)) {
                        if (coldstart) {
                            TimestampedState thierState = otherE.getValue();
                            storage.compute(new Key(otherE.getKey().a, member, otherE.getKey().context), (k, myState) -> {
                                if (myState != null && myState.timestamp == thierState.timestamp && myState.state == thierState.state) {
                                    return myState;
                                } else {
                                    return new TimestampedState(thierState.state, thierState.timestamp, idProvider.nextId());
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

                        TimestampedState thierState = otherE.getValue();
                        storage.compute(e.getKey(), (k, myState) -> {
                            if (myState != null && myState.timestamp == thierState.timestamp && myState.state == thierState.state) {
                                return myState;
                            } else {
                                return new TimestampedState(thierState.state, thierState.timestamp, idProvider.nextId());
                            }
                        });

                    }
                }
                if (otherE != null && coldstart) {
                    TimestampedState thierState = otherE.getValue();
                    storage.compute(new Key(otherE.getKey().a, member, otherE.getKey().context), (k, myState) -> {
                        if (myState != null && myState.timestamp == thierState.timestamp && myState.state == thierState.state) {
                            return myState;
                        } else {
                            return new TimestampedState(thierState.state, thierState.timestamp, idProvider.nextId());
                        }
                    });
                }

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

    static class TimestampedState {

        final State state;
        final long timestamp;
        final long version;

        public TimestampedState(State state, long timestamp, long version) {
            this.state = state;
            this.timestamp = timestamp;
            this.version = version;
        }

        @Override
        public String toString() {
            return "TimestampedState{" + "state=" + state + ", timestamp=" + timestamp + ", version=" + version + '}';
        }

    }

}
