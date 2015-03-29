package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.Filer;
import java.io.File;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowIOProvider implements RowIOProvider {

    private final IoStats ioStats;

    public BinaryRowIOProvider(IoStats ioStats) {
        this.ioStats = ioStats;
    }

    @Override
    public RowIO create(File dir, String name) throws Exception {
        dir.mkdirs();
        File file = new File(dir, name);
        Filer filer = new Filer(file.getAbsolutePath(), "rw");
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO(file, filer, rowReader, rowWriter);
    }

}
