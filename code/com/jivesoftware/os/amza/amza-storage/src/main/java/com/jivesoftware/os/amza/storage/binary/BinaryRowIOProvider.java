package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.storage.filer.Filer;
import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowIOProvider implements RowIOProvider {

    @Override
    public RowIO create(File dir, String name) throws Exception {
        dir.mkdirs();
        File file = new File(dir, name);
        Filer filer = new Filer(file.getAbsolutePath(), "rw");
        BinaryRowReader rowReader = new BinaryRowReader(filer);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer);
        return new BinaryRowIO(file, filer, rowReader, rowWriter);
    }

}
