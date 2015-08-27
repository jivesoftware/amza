package com.jivesoftware.os.amza.aquarium;

import java.util.Objects;

/**
 *
 * @author jonathan.colt
 */
public interface ReadWaterline {

    Waterline get();

    void getOthers(StreamQuorumState stream) throws Exception;

    void acknowledgeOther() throws Exception;

    interface StreamQuorumState {

        boolean stream(Waterline waterline) throws Exception;
    }

    static class Waterline implements Comparable<Waterline> {

        private final Member member;
        private final State state;
        private final long timestamp;
        private final boolean hasQuorum;

        public Waterline(Member member, State state, long timestamp, boolean hasQuorum) {
            this.member = member;
            this.state = state;
            this.timestamp = timestamp;
            this.hasQuorum = hasQuorum;
        }

        @Override
        public int compareTo(Waterline o) {
            int c = -Long.compare(timestamp, o.timestamp);
            if (c != 0) {
                return c;
            }
            c = -Boolean.compare(hasQuorum, o.hasQuorum);
            if (c != 0) {
                return c;
            }
            c = state.compareTo(o.state);
            if (c != 0) {
                return c;
            }
            return member.compareTo(o.member);
        }

        public Member getMember() {
            return member;
        }

        public State getState() {
            return state;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isHasQuorum() {
            return hasQuorum;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 29 * hash + Objects.hashCode(this.member);
            hash = 29 * hash + Objects.hashCode(this.state);
            hash = 29 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
            hash = 29 * hash + (this.hasQuorum ? 1 : 0);
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
            final Waterline other = (Waterline) obj;
            if (!Objects.equals(this.member, other.member)) {
                return false;
            }
            if (this.state != other.state) {
                return false;
            }
            if (this.timestamp != other.timestamp) {
                return false;
            }
            if (this.hasQuorum != other.hasQuorum) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "Waterline{" + "member=" + member + ", state=" + state + ", version=" + timestamp + ", hasQuorum=" + hasQuorum + '}';
        }

    }
}
