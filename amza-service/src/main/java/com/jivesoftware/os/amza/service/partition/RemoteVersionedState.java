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
    public String toString() {
        return "RemoteVersionedState{" + "waterline=" + waterline + ", version=" + version + '}';
    }

}
