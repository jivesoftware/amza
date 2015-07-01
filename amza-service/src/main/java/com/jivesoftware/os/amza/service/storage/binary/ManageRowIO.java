package com.jivesoftware.os.amza.service.storage.binary;

/**
 *
 * @author jonathan.colt
 */
public interface ManageRowIO<K> {

    void move(K from, K to) throws Exception;

    void delete(K key) throws Exception;
}
