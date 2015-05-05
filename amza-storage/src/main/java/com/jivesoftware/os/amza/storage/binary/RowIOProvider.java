package com.jivesoftware.os.amza.storage.binary;

import java.io.File;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public interface RowIOProvider {

    RowIO create(File dir, String name) throws Exception;

    List<File> listExisting(File dir);
}
