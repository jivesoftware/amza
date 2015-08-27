/*
 * Copyright 2015 Jive Software Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.shared.partition;

import com.jivesoftware.os.amza.api.ring.RingMember;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public class PartitionMemberState implements Comparable<PartitionMemberState> {

    private final RingMember ringMember;
    private final State desired;
    private final State current;

    public PartitionMemberState(RingMember ringMember, State desired, State current) {
        this.ringMember = ringMember;
        this.desired = desired;
        this.current = current;
    }

    @Override
    public int compareTo(PartitionMemberState o) {
        return ringMember.compareTo(o.ringMember);
    }

    public static List<PartitionMemberState> progress(RingMember ringMember, List<PartitionMemberState> ringStates) {
        List<PartitionMemberState> progress = new ArrayList<>(ringStates.size());
        for (PartitionMemberState ringState : ringStates) {
            if (ringState.ringMember.equals(ringMember)) {
                State desired = ringState.desired;
                PartitionMemberState transition = ringState.current.node.transition(ringMember, desired, ringStates);
                if (transition != null) {
                    progress.add(transition);
                }
            } else {
                progress.add(ringState);
            }
        }
        return progress;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 41 * hash + Objects.hashCode(this.ringMember);
        hash = 41 * hash + Objects.hashCode(this.desired);
        hash = 41 * hash + Objects.hashCode(this.current);
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
        final PartitionMemberState other = (PartitionMemberState) obj;
        if (!Objects.equals(this.ringMember, other.ringMember)) {
            return false;
        }
        if (this.desired != other.desired) {
            return false;
        }
        if (this.current != other.current) {
            return false;
        }
        return true;
    }

    public static enum State {

        ketchup(false, new Ketchup()),
        elect_leader(true, new ElectLeader()),
        writable_leader(true, new WritableLeader()),
        writable_follower(false, new WritableFollower()),
        read_only(false, new ReadOnly());

        Node node;
        boolean leadershipTrack;

        private State(boolean leadershipTrack, Node node) {
            this.node = node;
            this.leadershipTrack = leadershipTrack;
        }

    }

    /*
     // TODO include expunged
     t=   1         2        3a 3b      4            5         6            7        8          9            10        11

     ACTUAL

     1.  ketchup          > leader     > demoted              > follower                     > ketchup              > follower
     2.  ketchup                       > nominated  > leader                      > demoted                         > follower
     3.  ketchup          > follower   > ketchup              > follower                     > nominated  > leader

     DESIRED

     1.          > (1)leader
     .             (2)follower
     .             (3)follower

     2.                      > (1)follower
     .                         (2)leader
     .                         (3)follower

     3.                                                                  > (1)follower
     .                                                                     (2)follower
     .                                                                     (3)leader

     */


    
    // Offline -> ElectLeader -> BecomeLeader
    static class Ketchup implements Node {

        @Override
        public PartitionMemberState transition(RingMember ringMember, State desired, List<PartitionMemberState> ringStates) {
            if (desired == State.ketchup) {
                for (PartitionMemberState ringState : ringStates) {
                    if (ringState.desired.leadershipTrack) {
                        return new PartitionMemberState(ringMember, State.writable_follower, State.ketchup);
                    }
                }
                return new PartitionMemberState(ringMember, State.writable_leader, State.elect_leader);
            } else if (desired.leadershipTrack) {
                for (PartitionMemberState ringState : ringStates) {
                    if (ringState.desired.leadershipTrack && !ringState.ringMember.equals(ringMember)) {
                        return new PartitionMemberState(ringMember, State.writable_follower, State.ketchup);
                    }
                }
                return new PartitionMemberState(ringMember, State.writable_leader, State.elect_leader);
            } else {
                for (PartitionMemberState ringState : ringStates) {
                    if (ringState.desired == State.writable_leader && ringState.current == State.writable_leader) {
                        return new PartitionMemberState(ringMember, State.writable_follower, State.writable_follower);
                    }
                }
                return new PartitionMemberState(ringMember, State.writable_follower, State.ketchup);
            }
        }
    }

    static class ElectLeader implements Node {

        @Override
        public PartitionMemberState transition(RingMember ringMember, State desired, List<PartitionMemberState> ringStates) {
            if (desired == State.writable_leader) {
                for (PartitionMemberState ringState : ringStates) {
                    if (ringState.desired.leadershipTrack) {
                        return new PartitionMemberState(ringMember, State.writable_follower, State.ketchup);
                    }
                }
                return new PartitionMemberState(ringMember, State.writable_leader, State.elect_leader);
            }
            return null;
        }

    }

    static class WritableLeader implements Node {

        @Override
        public PartitionMemberState transition(RingMember ringMember, State desired, List<PartitionMemberState> ringStates) {
            return null;
        }

    }

    static class WritableFollower implements Node {

        @Override
        public PartitionMemberState transition(RingMember ringMember, State desired, List<PartitionMemberState> ringStates) {
            return null;
        }
    }

    static class ReadOnly implements Node {

        @Override
        public PartitionMemberState transition(RingMember ringMember, State desired, List<PartitionMemberState> ringStates) {
            return null;
        }
    }

    static interface Node {

        PartitionMemberState transition(RingMember ringMember, State desired, List<PartitionMemberState> ringStates);
    }

}
