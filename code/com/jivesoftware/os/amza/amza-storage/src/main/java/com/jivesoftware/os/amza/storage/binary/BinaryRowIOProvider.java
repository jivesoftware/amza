package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.storage.filer.Filer;
import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowIOProvider implements RowIOProvider {

    private final static String VERSION_1 = "v1";

    @Override
    public RowIO create(File dir, String name) throws Exception {
        File versionedDir = new File(dir, VERSION_1);
        versionedDir.mkdirs();

        File file = new File(versionedDir, name);
        Filer filer = new Filer(file.getAbsolutePath(), "rw");
        BinaryRowReader rowReader = new BinaryRowReader(filer);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer);
        return new BinaryRowIO(file, filer, rowReader, rowWriter);
    }

}
