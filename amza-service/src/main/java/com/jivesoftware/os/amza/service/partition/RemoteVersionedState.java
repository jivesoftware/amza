package com.jivesoftware.os.amza.service.partition;

import com.jivesoftware.os.aquarium.Waterline;

/**
 * @author jonathan.colt
 */
public class RemoteVersionedState {

    public final Waterline waterline;
    public final long version;

    public RemoteVersionedState(Waterline waterline, long version) {
        this.waterline = waterline;
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
        return waterline == that.waterline;
    }

    @Override
    public int hashCode() {
        int result = waterline != null ? waterline.hashCode() : 0;
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "RemoteVersionedState{" + "waterline=" + waterline + ", version=" + version + '}';
    }

}
