package com.jivesoftware.os.amza.storage.binary;

import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.stats.IoStats;
import com.jivesoftware.os.amza.storage.filer.MemoryBackedWALFiler;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MemoryBackedRowIOProvider implements RowIOProvider {

    private final IoStats ioStats;
    private final int corruptionParanoiaFactor;

    public MemoryBackedRowIOProvider(IoStats ioStats, int corruptionParanoiaFactor) {
        this.ioStats = ioStats;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
    }

    @Override
    public RowIO<MemoryBackedWALFiler> create(File key, String name) throws Exception {
        MemoryBackedWALFiler filer = new MemoryBackedWALFiler(new HeapFiler());
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats, corruptionParanoiaFactor);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO<MemoryBackedWALFiler>(new ManageMemoryRowIO(), filer, rowReader, rowWriter);
    }

    @Override
    public List<File> listExisting(File dir) {
        File[] existing = dir.listFiles();
        return existing != null ? Arrays.asList(existing) : Collections.<File>emptyList();
    }
}
