package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.amza.service.stats.IoStats;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class BinaryRowIOProvider implements RowIOProvider<File> {

    private final String[] workingDirectories;
    private final IoStats ioStats;
    private final int corruptionParanoiaFactor;
    private final boolean useMemMap;

    public BinaryRowIOProvider(String[] workingDirectories, IoStats ioStats, int corruptionParanoiaFactor, boolean useMemMap) {
        this.workingDirectories = workingDirectories;
        this.ioStats = ioStats;
        this.corruptionParanoiaFactor = corruptionParanoiaFactor;
        this.useMemMap = useMemMap;
    }

    @Override
    public File baseKey(VersionedPartitionName versionedPartitionName) {
        return new File(workingDirectories[Math.abs(versionedPartitionName.hashCode() % workingDirectories.length)]);
    }

    @Override
    public RowIO<File> create(File dir, String name) throws Exception {
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed trying to mkdirs for " + dir);
        }
        File file = new File(dir, name);
        DiskBackedWALFiler filer = new DiskBackedWALFiler(file.getAbsolutePath(), "rw", useMemMap);
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats, corruptionParanoiaFactor);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO<>(file, rowReader, rowWriter);
    }

    @Override
    public List<String> listExisting(File dir) {
        File[] existing = dir.listFiles();
        return existing != null ? Lists.transform(Arrays.asList(existing), File::getName) : Collections.<String>emptyList();
    }

    @Override
    public File versionedKey(File baseKey, String latestVersion) throws Exception {
        File directory = new File(baseKey, latestVersion);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Failed trying to mkdirs for " + directory);
        }
        return directory;
    }

    @Override
    public boolean ensure(File key) {
        return key.exists() || key.mkdirs();
    }

    @Override
    public File buildKey(File versionedKey, String name) {
        return new File(versionedKey, name);
    }

    @Override
    public File createTempKey() {
        return Files.createTempDir();
    }

    @Override
    public void moveTo(File key, File to) throws Exception {
        File destinationFile = new File(to, key.getName());
        Files.move(key, destinationFile);
    }

    @Override
    public void delete(File key) throws Exception {
        FileUtils.deleteQuietly(key);
    }
}
