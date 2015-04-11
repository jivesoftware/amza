package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.WALFiler;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class BinaryRowIOProvider implements RowIOProvider {

    private final IoStats ioStats;
    private final int corruptionParanoiaFactor;

    public BinaryRowIOProvider(IoStats ioStats, int corruptionParanoiaFactor) {
        this.ioStats = ioStats;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
    }

    @Override
    public RowIO create(File dir, String name) throws Exception {
        dir.mkdirs();
        File file = new File(dir, name);
        WALFiler filer = new WALFiler(file.getAbsolutePath(), "rw");
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats, corruptionParanoiaFactor);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO(file, filer, rowReader, rowWriter);
    }

    @Override
    public List<File> listExisting(File dir) {
        File[] existing = dir.listFiles();
        return existing != null ? Arrays.asList(existing) : Collections.<File>emptyList();
    }
}
