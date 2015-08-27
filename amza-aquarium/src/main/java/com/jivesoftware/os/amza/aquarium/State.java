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
            if (desiredLeader != null && desiredLeader.isHasQuorum()) {
                boolean[] hasLeader = {false};
                boolean[] hasNominated = {false};
                readCurrent.getOthers((other) -> {
                    if (atDesiredState(leader, other, desiredLeader)) {
                        hasLeader[0] = true;
                    }

                    if (other.getState() == nominated && other.isHasQuorum() && desiredLeader.getMember().equals(other.getMember())) {
                        hasNominated[0] = true;
                    }
                    return !hasLeader[0] && !hasNominated[0];
                });

                if (!desiredLeader.getMember().equals(current.getMember())) {
                    if (desired.getState() == follower) {
                        if (desired.isHasQuorum() && hasLeader[0]) {
                            transitionCurrent.transition(current, desired.getTimestamp(), follower);
                        }
                    } else {
                        transitionDesired.transition(current, desired.getTimestamp(), follower);
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
                || !desiredLeader.isHasQuorum()
                || !currentLeader.equals(desiredLeader)
                || !current.equals(desired)) {
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
                || !desiredLeader.isHasQuorum()
                || !currentLeader.equals(desiredLeader)) {
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
                if (desiredLeader.isHasQuorum()
                    && desiredLeader.equals(currentLeader)) {
                    return transitionCurrent.transition(current, desired.getTimestamp(), inactive);
                }
                if (desiredLeader.isHasQuorum()
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
        if (recoverFromLackOfDesired(current, desired, readDesired, transitionDesired)) {
            return true;
        }
        return !current.isHasQuorum() || !desired.isHasQuorum();
    }

    static boolean atDesiredState(State state, Waterline current, Waterline desired) {
        return (desired.getState() == state
            && desired.isHasQuorum()
            && desired.equals(current));
    }

    static Waterline highest(State state, ReadWaterline readWaterline, Waterline me) throws Exception {
        Waterline[] waterline = {null};
        ReadWaterline.StreamQuorumState stream = (other) -> {
            if (other.getState() == state) {
                if (waterline[0] == null) {
                    waterline[0] = other;
                } else {
                    if (waterline[0].compareTo(other) > 0) {
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

    // "a.k.a delete facebook and hit the gym. a.a.k.a plenty of fish in the sea.", said Kevin. (circa 2015)
    static boolean recoverFromLackOfDesired(Waterline current, Waterline desired, ReadWaterline readDesired, TransitionQuorum transitionDesired) throws
        Exception {
        if (desired == null) {
            Waterline desiredLeader = highest(leader, readDesired, desired);
            if (desiredLeader != null && desiredLeader.getMember().equals(current.getMember())) {
                return true;
            }
            transitionDesired.transition(current, current.getTimestamp(), (desiredLeader != null) ? follower : leader);
            return true;
        }
        return false;
    }
}
