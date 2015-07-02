package com.jivesoftware.os.amza.service.storage.binary;

import java.io.File;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface RowIOProvider {

    <K> RowIO<K> create(File dir, String name) throws Exception;

    List<File> listExisting(File dir);
}
