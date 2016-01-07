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

    void moveTo(K key, K to) throws Exception;

    void delete(K key) throws Exception;

    boolean ensure(K key);

    boolean exists(K key);
}
