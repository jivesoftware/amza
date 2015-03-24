package com.jivesoftware.os.amza.storage.binary;

import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public interface RowIOProvider {

    RowIO create(File dir, String name) throws Exception;
}
