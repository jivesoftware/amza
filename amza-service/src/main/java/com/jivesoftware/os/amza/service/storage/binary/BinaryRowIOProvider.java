package com.jivesoftware.os.amza.service.storage.binary;

import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
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
    private final boolean useMemMap;

    public BinaryRowIOProvider(IoStats ioStats, int corruptionParanoiaFactor, boolean useMemMap) {
        this.ioStats = ioStats;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
        this.useMemMap = useMemMap;
    }

    @Override
    public RowIO<File> create(File dir, String name) throws Exception {
        dir.mkdirs();
        File file = new File(dir, name);
        DiskBackedWALFiler filer = new DiskBackedWALFiler(file.getAbsolutePath(), "rw", useMemMap);
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats, corruptionParanoiaFactor);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO<>(new ManageFileRowIO(), file, rowReader, rowWriter);
    }

    @Override
    public List<File> listExisting(File dir) {
        File[] existing = dir.listFiles();
        return existing != null ? Arrays.asList(existing) : Collections.<File>emptyList();
    }
}
