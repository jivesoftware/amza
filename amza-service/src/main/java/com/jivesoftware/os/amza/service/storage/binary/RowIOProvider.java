package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.api.wal.RowIO;
import java.io.File;
import java.util.List;

/**
 * @author jonathan.colt
 */
public interface RowIOProvider {

    RowIO open(File key, String name, boolean createIfAbsent, int updatesBetweenLeaps, int maxLeaps) throws Exception;

    List<String> listExisting(File key);

    File versionedKey(File baseKey, String latestVersion) throws Exception;

    File buildKey(File versionedKey, String name) throws Exception;

    void moveTo(File fromKey, String fromName, File toKey, String toName) throws Exception;

    void safeMoveTo(File fromKey, String fromName, File toKey, String toName) throws Exception;

    void delete(File key, String name) throws Exception;

    boolean ensureKey(File key);

    boolean exists(File key, String name);
}
