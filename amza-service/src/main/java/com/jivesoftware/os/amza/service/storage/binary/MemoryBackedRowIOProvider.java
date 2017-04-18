package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.wal.RowIO;
import com.jivesoftware.os.amza.service.filer.ByteBufferFactory;
import com.jivesoftware.os.amza.service.filer.MultiAutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.amza.service.storage.filer.MemoryBackedWALFiler;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author jonathan.colt
 */
public class MemoryBackedRowIOProvider implements RowIOProvider {

    private final long initialBufferSegmentSize;
    private final long maxBufferSegmentSize;
    private final int defaultUpdatesBetweenLeaps;
    private final int defaultMaxLeaps;
    private final ByteBufferFactory byteBufferFactory;

    private final Map<File, MemoryBackedWALFiler> filers = Maps.newConcurrentMap();

    public MemoryBackedRowIOProvider(
        long initialBufferSegmentSize,
        long maxBufferSegmentSize,
        int defaultUpdatesBetweenLeaps,
        int defaultMaxLeaps,
        ByteBufferFactory byteBufferFactory) {
        this.initialBufferSegmentSize = initialBufferSegmentSize;
        this.maxBufferSegmentSize = maxBufferSegmentSize;
        this.defaultUpdatesBetweenLeaps = defaultUpdatesBetweenLeaps;
        this.defaultMaxLeaps = defaultMaxLeaps;
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
    public RowIO open(File key, String name, boolean createIfAbsent, int updatesBetweenLeaps, int maxLeaps) throws Exception {
        File file = new File(key, name);
        MemoryBackedWALFiler filer = getFiler(file);
        BinaryRowReader rowReader = new BinaryRowReader(filer);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer);
        return new BinaryRowIO(key,
            name,
            rowReader,
            rowWriter,
            updatesBetweenLeaps > 0 ? updatesBetweenLeaps : defaultUpdatesBetweenLeaps,
            maxLeaps > 0 ? maxLeaps : defaultMaxLeaps);
    }

    @Override
    public List<String> listExisting(File key) {
        return Collections.emptyList();
    }

    @Override
    public File versionedKey(File baseKey, String latestVersion) throws Exception {
        return new File(baseKey, latestVersion);
    }

    @Override
    public File buildKey(File versionedKey, String name) throws Exception {
        return new File(versionedKey, name);
    }

    private MemoryBackedWALFiler createFiler() throws IOException {
        return new MemoryBackedWALFiler(new MultiAutoGrowingByteBufferBackedFiler(initialBufferSegmentSize, maxBufferSegmentSize, byteBufferFactory));
    }

    @Override
    public void moveTo(File fromKey, String fromName, File toKey, String toName) throws Exception {
        MemoryBackedWALFiler filer = filers.remove(new File(fromKey, fromName));
        filers.put(new File(toKey, toName), filer);
    }

    @Override
    public void delete(File key, String name) throws Exception {
        filers.remove(new File(key, name));
    }

    @Override
    public boolean ensureKey(File key) {
        return true;
    }

    @Override
    public boolean exists(File key, String name) {
        return true;
    }

    @Override
    public long sizeInBytes(File key, String name) {
        return 0;
    }
}
