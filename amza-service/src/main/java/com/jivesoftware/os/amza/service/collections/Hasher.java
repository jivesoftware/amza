package com.jivesoftware.os.amza.service.collections;

/**
 *
 * @author jonathan.colt
 */
public interface Hasher<K> {

    int hashCode(K key, int offset, int length);

}
