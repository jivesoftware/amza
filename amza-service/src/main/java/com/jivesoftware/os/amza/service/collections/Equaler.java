package com.jivesoftware.os.amza.service.collections;

/**
 *
 * @author jonathan.colt
 */
public interface Equaler<K> {

    boolean equals(K a, K b, int offset, int length);

}
