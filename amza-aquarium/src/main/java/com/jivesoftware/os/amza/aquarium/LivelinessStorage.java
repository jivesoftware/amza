package com.jivesoftware.os.amza.aquarium;

/**
 *
 */
public interface LivelinessStorage {

    boolean scan(Member rootMember, Member otherMember, LivelinessStream stream) throws Exception;

    boolean update(LivelinessUpdates updates) throws Exception;

    long get(Member rootMember, Member otherMember) throws Exception;

    interface LivelinessStream {

        boolean stream(Member rootMember, boolean isSelf, Member ackMember, long timestamp, long version) throws Exception;
    }

    interface LivelinessUpdates {

        boolean updates(SetLiveliness setLiveliness) throws Exception;
    }

    interface SetLiveliness {

        boolean set(Member rootMember, Member otherMember, long timestamp) throws Exception;
    }

}
