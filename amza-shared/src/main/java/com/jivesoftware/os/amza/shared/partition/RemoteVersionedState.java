package com.jivesoftware.os.amza.shared.partition;

import com.jivesoftware.os.amza.aquarium.State;

/**
 * @author jonathan.colt
 */
public class RemoteVersionedState {

    public final State state;
    public final long version;

    public RemoteVersionedState(State state, long version) {
        this.state = state;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RemoteVersionedState that = (RemoteVersionedState) o;

        if (version != that.version) {
            return false;
        }
        if (state != that.state) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "RemoteVersionedState{" + "state=" + state + ", version=" + version + '}';
    }

}
