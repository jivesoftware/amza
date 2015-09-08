package com.jivesoftware.os.amza.aquarium;

/**
 *
 */
public interface StateStorage<T> {

    boolean scan(Member rootMember, Member otherMember, T lifecycle, StateStream<T> stream) throws Exception;

    boolean update(StateUpdates<T> updates) throws Exception;

    interface StateStream<T> {

        boolean stream(Member rootMember, boolean isSelf, Member ackMember, T lifecycle, State state, long timestamp, long version) throws Exception;
    }

    interface StateUpdates<T> {

        boolean updates(SetState<T> setState) throws Exception;
    }

    interface SetState<T> {

        boolean set(Member rootMember, Member otherMember, T lifecycle, State state, long timestamp) throws Exception;
    }

}
