package com.jivesoftware.os.amza.aquarium;

/**
 *
 */
public enum State {

    bootstrap((byte) 1, new Bootstrap()),
    inactive((byte) 2, new Inactive()),
    nominated((byte) 3, new Nominated()),
    leader((byte) 4, new Leader()),
    follower((byte) 5, new Follower()),
    demoted((byte) 6, new Demoted()),
    expunged((byte) 7, new Expunged());

    Transistor transistor;
    byte serializedForm;

    State(byte serializedForm, Transistor transistor) {
        this.transistor = transistor;
        this.serializedForm = serializedForm;
    }

    public byte getSerializedForm() {
        return serializedForm;
    }

    public static State fromSerializedForm(byte serializedForm) {
        for (State state : values()) {
            if (state.serializedForm == serializedForm) {
                return state;
            }
        }
        return null;
    }

    interface Transistor {

        boolean advance(CurrentTimeMillis currentTimeMillis,
            Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception;
    }

    static class Bootstrap implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis,
            Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {

            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }
            return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
        }
    }

    static class Inactive implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis,
            Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }

            if (desired.getState() == expunged) {
                transitionCurrent.transition(current, desired.getTimestamp(), expunged);
                return false;
            }

            Waterline desiredLeader = highest(currentTimeMillis, leader, readDesired, desired);
            if (desiredLeader != null && desiredLeader.isAtQuorum()) {
                boolean[] hasLeader = {false};
                boolean[] hasNominated = {false};
                readCurrent.getOthers((other) -> {
                    if (atDesiredState(currentTimeMillis, leader, other, desiredLeader)) {
                        hasLeader[0] = true;
                    }

                    if (other.getState() == nominated && other.isAtQuorum() && desiredLeader.getMember().equals(other.getMember())) {
                        hasNominated[0] = true;
                    }
                    return !hasLeader[0] && !hasNominated[0];
                });

                if (!desiredLeader.getMember().equals(current.getMember())) {
                    if (desired.getState() == follower) {
                        if (desired.isAtQuorum() && hasLeader[0]) {
                            transitionCurrent.transition(current, desired.getTimestamp(), follower);
                        }
                    } else {
                        transitionDesired.transition(desired, desired.getTimestamp(), follower);
                    }
                    return false;
                } else if (hasNominated[0]) {
                    return false;
                } else {
                    if (desired.getState() == leader) {
                        return transitionCurrent.transition(current, desired.getTimestamp(), nominated);
                    }
                }
            }
            return false;
        }
    }

    static class Nominated implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis, Waterline current, ReadWaterline readCurrent, TransitionQuorum transitionCurrent,
            Waterline desired, ReadWaterline readDesired, TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline desiredLeader = highest(currentTimeMillis, leader, readDesired, desired);
            if (desiredLeader == null || !desired.getMember().equals(desiredLeader.getMember())) {
                return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
            } else {
                if (desired.getTimestamp() < desiredLeader.getTimestamp()) {
                    return false;
                }
                if (desired.getState() != leader) {
                    return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
                }
                return transitionCurrent.transition(current, desiredLeader.getTimestamp(), leader);
            }
        }

    }

    static class Follower implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis, Waterline current, ReadWaterline readCurrent, TransitionQuorum transitionCurrent,
            Waterline desired, ReadWaterline readDesired, TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline currentLeader = highest(currentTimeMillis, leader, readCurrent, current);
            Waterline desiredLeader = highest(currentTimeMillis, leader, readDesired, desired);
            if (currentLeader == null
                || desiredLeader == null
                || !currentLeader.isAtQuorum()
                || !checkEquals(currentTimeMillis, currentLeader, desiredLeader)
                || !checkEquals(currentTimeMillis, current, desired)) {
                return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
            }
            return false;
        }

    }

    static class Leader implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis, Waterline current, ReadWaterline readCurrent, TransitionQuorum transitionCurrent,
            Waterline desired, ReadWaterline readDesired, TransitionQuorum transitionDesired) throws Exception {

            if (!current.isAlive(currentTimeMillis.get())) {
                // forge a unique, larger timestamp to chase
                long desiredTimestamp = (desired != null ? desired.getTimestamp() : current.getTimestamp()) + 1;
                if (desired == null || desired.getState() == leader) {
                    transitionDesired.transition(desired, desiredTimestamp, follower);
                    return false;
                }
            }
            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline currentLeader = highest(currentTimeMillis, leader, readCurrent, current);
            Waterline desiredLeader = highest(currentTimeMillis, leader, readDesired, desired);
            if (currentLeader == null
                || desiredLeader == null
                || !desiredLeader.getMember().equals(current.getMember())) {
                transitionDesired.transition(desired, desired.getTimestamp(), follower);
            }

            if (currentLeader == null
                || desiredLeader == null
                || !desiredLeader.isAtQuorum()
                || !checkEquals(currentTimeMillis, currentLeader, desiredLeader)) {
                return transitionCurrent.transition(current, desired.getTimestamp(), demoted);
            }
            return false;
        }

    }

    static class Demoted implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis, Waterline current, ReadWaterline readCurrent, TransitionQuorum transitionCurrent,
            Waterline desired, ReadWaterline readDesired, TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline desiredLeader = highest(currentTimeMillis, leader, readDesired, desired);
            if (desiredLeader != null) {

                Waterline currentLeader = highest(currentTimeMillis, leader, readCurrent, current);
                if (desiredLeader.isAtQuorum()
                    && checkEquals(currentTimeMillis, desiredLeader, currentLeader)) {
                    return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
                }
                if (desiredLeader.isAtQuorum()
                    && desiredLeader.getTimestamp() >= current.getTimestamp()
                    && desiredLeader.getMember().equals(current.getMember())) {
                    return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
                }
            }
            return false;
        }

    }

    static class Expunged implements Transistor {

        @Override
        public boolean advance(CurrentTimeMillis currentTimeMillis, Waterline current, ReadWaterline readCurrent, TransitionQuorum transitionCurrent,
            Waterline desired, ReadWaterline readDesired, TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(currentTimeMillis, current, desired, readDesired, transitionDesired)) {
                return false;
            }

            if (desired.getState() != expunged) {
                return transitionCurrent.transition(current, desired.getTimestamp(), bootstrap);
            }
            return false;
        }

    }

    static boolean recoverOrAwaitingQuorum(CurrentTimeMillis currentTimeMillis,
        Waterline current,
        Waterline desired,
        ReadWaterline readDesired,
        TransitionQuorum transitionDesired) throws Exception {

        if (!current.isAlive(currentTimeMillis.get())) {
            return true;
        }

        Waterline desiredLeader = highest(currentTimeMillis, leader, readDesired, desired);
        boolean leaderIsLively = desiredLeader != null && desiredLeader.isAlive(currentTimeMillis.get());
        if (desired == null) {
            // recover from lack of desired
            // "a.k.a delete facebook and hit the gym. a.a.k.a plenty of fish in the sea.", said Kevin. (circa 2015)
            if (desiredLeader != null && desiredLeader.getMember().equals(current.getMember())) {
                return true;
            }
            Waterline forged = new Waterline(current.getMember(), bootstrap, current.getTimestamp(), current.getVersion(), false, -1);
            State desiredState = (leaderIsLively || !current.isAlive(currentTimeMillis.get())) ? follower : leader;
            long desiredTimestamp = (desiredState == leader && desiredLeader != null) ? desiredLeader.getTimestamp() + 1 : current.getTimestamp();
            transitionDesired.transition(forged, desiredTimestamp, desiredState);
            return true;
        }

        if (!leaderIsLively && current.isAlive(currentTimeMillis.get()) && desired.getState() != leader && desired.getState() != expunged) {
            long desiredTimestamp = (desiredLeader != null) ? desiredLeader.getTimestamp() + 1 : desired.getTimestamp();
            transitionDesired.transition(desired, desiredTimestamp, leader);
            return true;
        }

        return !current.isAtQuorum() || !desired.isAtQuorum();
    }

    static boolean atDesiredState(CurrentTimeMillis currentTimeMillis, State state, Waterline current, Waterline desired) {
        return (desired.getState() == state
            && desired.isAtQuorum()
            && checkEquals(currentTimeMillis, desired, current));
    }

    static Waterline highest(CurrentTimeMillis currentTimeMillis, State state, ReadWaterline readWaterline, Waterline me) throws Exception {
        @SuppressWarnings("unchecked")
        Waterline[] waterline = new Waterline[1];
        StreamQuorumState stream = (other) -> {
            if (other.getState() == state) {
                if (waterline[0] == null) {
                    waterline[0] = other;
                } else {
                    if (compare(currentTimeMillis, waterline[0], other) > 0) {
                        waterline[0] = other;
                    }
                }
            }
            return true;
        };
        if (me != null) {
            stream.stream(me);
        }
        readWaterline.getOthers(stream);
        return waterline[0];
    }

    static boolean checkEquals(CurrentTimeMillis currentTimeMillis, Waterline a, Waterline b) {
        if (a == b) {
            return true;
        }
        if (b == null) {
            return false;
        }
        if (a.getTimestamp() != b.getTimestamp()) {
            return false;
        }
        // don't compare version since they're always unique
        if (a.isAtQuorum() != b.isAtQuorum()) {
            return false;
        }
        long timestamp = currentTimeMillis.get();
        if (a.isAlive(timestamp) != b.isAlive(timestamp)) {
            return false;
        }
        if (a.getMember() != null ? !a.getMember().equals(b.getMember()) : b.getMember() != null) {
            return false;
        }
        return !(a.getState() != null ? !a.getState().equals(b.getState()) : b.getState() != null);
    }

    static int compare(CurrentTimeMillis currentTimeMillis, Waterline a, Waterline b) {
        int c = -Long.compare(a.getTimestamp(), b.getTimestamp());
        if (c != 0) {
            return c;
        }
        c = -Long.compare(a.getVersion(), b.getVersion());
        if (c != 0) {
            return c;
        }
        c = -Boolean.compare(a.isAtQuorum(), b.isAtQuorum());
        if (c != 0) {
            return c;
        }
        long timstamp = currentTimeMillis.get();
        c = -Boolean.compare(a.isAlive(timstamp), b.isAlive(timstamp));
        if (c != 0) {
            return c;
        }
        c = a.getState().compareTo(b.getState());
        if (c != 0) {
            return c;
        }
        return a.getMember().compareTo(b.getMember());
    }
}
