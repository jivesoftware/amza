package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import java.io.File;
import java.util.List;

/**
 * @author jonathan.colt
 */
public interface RowIOProvider<K> {

    K baseKey(VersionedPartitionName versionedPartitionName);

    RowIO<K> open(K key, String name, boolean createIfAbsent) throws Exception;

    List<String> listExisting(K key);

    K versionedKey(K baseKey, String latestVersion) throws Exception;

    K buildKey(K versionedKey, String name) throws Exception;

    K createTempKey() throws Exception;

    void moveTo(K fromKey, String fromName, K toKey, String toName) throws Exception;

    void delete(K key, String name) throws Exception;

    boolean ensureKey(K key);

    boolean exists(K key, String name);
}
