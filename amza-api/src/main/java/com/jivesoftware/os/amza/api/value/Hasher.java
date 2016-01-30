package com.jivesoftware.os.amza.api.value;

/**
 *
 * @author jonathan.colt
 */
public interface Hasher<K> {

    int hashCode(K key, int offset, int length);

}
