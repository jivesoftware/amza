package com.jivesoftware.os.amza.storage.binary;

import com.google.common.io.Files;
import java.io.File;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class ManageFileRowIO implements ManageRowIO<File> {

    @Override
    public void move(File from, File to) throws Exception {
        File destinationFile = new File(to, from.getName());
        Files.move(from, destinationFile);
    }

    @Override
    public void delete(File key) throws Exception {
        FileUtils.deleteQuietly(key);
    }

}
