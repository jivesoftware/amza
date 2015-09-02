package com.jivesoftware.os.amza.aquarium;

/**
 *
 */
public interface MemberLifecycle<T> {

    T get() throws Exception;

    T getOther(Member other) throws Exception;
}
