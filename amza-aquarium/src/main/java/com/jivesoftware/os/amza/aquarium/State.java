package com.jivesoftware.os.amza.aquarium;

import com.jivesoftware.os.amza.aquarium.ReadWaterline.Waterline;

/**
 *
 */
public enum State {

    bootstrap(new Bootstrap()),
    inactive(new Inactive()),
    nominated(new Nominated()),
    leader(new Leader()),
    follower(new Follower()),
    demoted(new Demoted()),
    expunged(new Expunged());

    Transistor transistor;

    State(Transistor transistor) {
        this.transistor = transistor;
    }

    interface Transistor {

        boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception;
    }

    static class Bootstrap implements Transistor {

        @Override
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {

            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }
            return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
        }
    }

    static class Inactive implements Transistor {

        @Override
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }

            if (desired.getState() == expunged) {
                transitionCurrent.transition(current, desired.getTimestamp(), expunged);
                return false;
            }

            Waterline desiredLeader = highest(leader, readDesired, desired);
            if (desiredLeader != null && desiredLeader.isAtQuorum()) {
                boolean[] hasLeader = { false };
                boolean[] hasNominated = { false };
                readCurrent.getOthers((other) -> {
                    if (atDesiredState(leader, other, desiredLeader)) {
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
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline desiredLeader = highest(leader, readDesired, desired);
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
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline currentLeader = highest(leader, readCurrent, current);
            Waterline desiredLeader = highest(leader, readDesired, desired);
            if (currentLeader == null
                || desiredLeader == null
                || !currentLeader.isAtQuorum()
                || !checkEquals(currentLeader, desiredLeader)
                || !checkEquals(current, desired)) {
                return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
            }
            return false;
        }

    }

    static class Leader implements Transistor {

        @Override
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {

            if (!current.isAlive()) {
                // forge a unique, larger timestamp to chase
                long desiredTimestamp = (desired != null ? desired.getTimestamp() : current.getTimestamp()) + 1;
                if (desired == null || desired.getState() == leader) {
                    transitionDesired.transition(desired, desiredTimestamp, follower);
                    return false;
                }
            }
            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline currentLeader = highest(leader, readCurrent, current);
            Waterline desiredLeader = highest(leader, readDesired, desired);
            if (currentLeader == null
                || desiredLeader == null
                || !desiredLeader.getMember().equals(current.getMember())) {
                transitionDesired.transition(desired, desired.getTimestamp(), follower);
            }

            if (currentLeader == null
                || desiredLeader == null
                || !desiredLeader.isAtQuorum()
                || !checkEquals(currentLeader, desiredLeader)) {
                return transitionCurrent.transition(current, desired.getTimestamp(), demoted);
            }
            return false;
        }

    }

    static class Demoted implements Transistor {

        @Override
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }

            Waterline desiredLeader = highest(leader, readDesired, desired);
            if (desiredLeader != null) {

                Waterline currentLeader = highest(leader, readCurrent, current);
                if (desiredLeader.isAtQuorum()
                    && checkEquals(desiredLeader, currentLeader)) {
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
        public boolean advance(Waterline current,
            ReadWaterline readCurrent,
            TransitionQuorum transitionCurrent,
            Waterline desired,
            ReadWaterline readDesired,
            TransitionQuorum transitionDesired) throws Exception {
            if (recoverOrAwaitingQuorum(current, desired, readDesired, transitionDesired)) {
                return false;
            }

            if (desired.getState() != expunged) {
                return transitionCurrent.transition(current, desired.getTimestamp(), bootstrap);
            }
            return false;
        }

    }

    static boolean recoverOrAwaitingQuorum(Waterline current,
        Waterline desired,
        ReadWaterline readDesired,
        TransitionQuorum transitionDesired) throws Exception {

        if (!current.isAlive()) {
            return true;
        }

        Waterline desiredLeader = highest(leader, readDesired, desired);
        boolean leaderIsLively = desiredLeader != null && desiredLeader.isAlive();
        if (desired == null) {
            // recover from lack of desired
            // "a.k.a delete facebook and hit the gym. a.a.k.a plenty of fish in the sea.", said Kevin. (circa 2015)
            if (desiredLeader != null && desiredLeader.getMember().equals(current.getMember())) {
                return true;
            }
            Waterline forged = new Waterline(current.getMember(), bootstrap, current.getTimestamp(), current.getVersion(), false, -1);
            State desiredState = (leaderIsLively || !current.isAlive()) ? follower : leader;
            long desiredTimestamp = (desiredState == leader && desiredLeader != null) ? desiredLeader.getTimestamp() + 1 : current.getTimestamp();
            transitionDesired.transition(forged, desiredTimestamp, desiredState);
            return true;
        }

        if (!leaderIsLively && current.isAlive() && desired.getState() != leader) {
            long desiredTimestamp = (desiredLeader != null) ? desiredLeader.getTimestamp() + 1 : desired.getTimestamp();
            transitionDesired.transition(desired, desiredTimestamp, leader);
            return true;
        }

        return !current.isAtQuorum() || !desired.isAtQuorum();
    }

    static boolean atDesiredState(State state, Waterline current, Waterline desired) {
        return (desired.getState() == state
            && desired.isAtQuorum()
            && checkEquals(desired, current));
    }

    static Waterline highest(State state, ReadWaterline readWaterline, Waterline me) throws Exception {
        @SuppressWarnings("unchecked")
        Waterline[] waterline = (Waterline[]) new Waterline[] { null };
        ReadWaterline.StreamQuorumState stream = (other) -> {
            if (other.getState() == state) {
                if (waterline[0] == null) {
                    waterline[0] = other;
                } else {
                    if (compare(waterline[0], other) > 0) {
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

    static boolean checkEquals(Waterline a, Waterline b) {
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
        if (a.isAlive() != b.isAlive()) {
            return false;
        }
        if (a.getMember() != null ? !a.getMember().equals(b.getMember()) : b.getMember() != null) {
            return false;
        }
        return !(a.getState() != null ? !a.getState().equals(b.getState()) : b.getState() != null);
    }

    static int compare(Waterline a, Waterline b) {
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
        c = -Boolean.compare(a.isAlive(), b.isAlive());
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
