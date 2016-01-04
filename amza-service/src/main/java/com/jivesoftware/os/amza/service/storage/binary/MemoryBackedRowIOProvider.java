package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import com.jivesoftware.os.amza.service.filer.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.amza.service.filer.ByteBufferFactory;
import com.jivesoftware.os.amza.service.stats.IoStats;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author jonathan.colt
 */
public class MemoryBackedRowIOProvider implements RowIOProvider<File> {

    private final String[] workingDirectories;
    private final IoStats ioStats;
    private final int corruptionParanoiaFactor;
    private final long initialBufferSegmentSize;
    private final long maxBufferSegmentSize;
    private final ByteBufferFactory byteBufferFactory;

    private final ConcurrentHashMap<File, MemoryBackedWALFiler> filers = new ConcurrentHashMap<>();

    public MemoryBackedRowIOProvider(String[] workingDirectories,
        IoStats ioStats,
        int corruptionParanoiaFactor,
        long initialBufferSegmentSize,
        long maxBufferSegmentSize,
        ByteBufferFactory byteBufferFactory) {
        this.workingDirectories = workingDirectories;
        this.ioStats = ioStats;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
        this.initialBufferSegmentSize = initialBufferSegmentSize;
        this.maxBufferSegmentSize = maxBufferSegmentSize;
        this.byteBufferFactory = byteBufferFactory;
    }

    private MemoryBackedWALFiler getFiler(File file) {
        return filers.computeIfAbsent(file, key1 -> {
            try {
                return createFiler();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public RowIO<File> create(File key, String name) throws Exception {
        File file = new File(key, name);
        MemoryBackedWALFiler filer = getFiler(file);
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats, corruptionParanoiaFactor);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO<>(file, rowReader, rowWriter);
    }

    @Override
    public List<String> listExisting(File key) {
        return Collections.emptyList();
    }

    @Override
    public File baseKey(VersionedPartitionName versionedPartitionName) {
        return new File(workingDirectories[Math.abs(versionedPartitionName.hashCode() % workingDirectories.length)]);
    }

    @Override
    public File versionedKey(File baseKey, String latestVersion) throws Exception {
        return new File(baseKey, latestVersion);
    }

    @Override
    public File buildKey(File versionedKey, String name) throws Exception {
        return new File(versionedKey, name);
    }

    @Override
    public File createTempKey() throws Exception {
        return Files.createTempDir();
    }

    private MemoryBackedWALFiler createFiler() throws IOException {
        return new MemoryBackedWALFiler(new AutoGrowingByteBufferBackedFiler(initialBufferSegmentSize, maxBufferSegmentSize, byteBufferFactory));
    }

    @Override
    public void moveTo(File key, File to) throws Exception {
        MemoryBackedWALFiler filer = filers.remove(key);
        filers.put(new File(to, key.getName()), filer);
    }

    @Override
    public void delete(File key) throws Exception {
        filers.remove(key);
    }

    @Override
    public boolean ensure(File key) {
        return true;
    }
}
