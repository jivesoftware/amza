package com.jivesoftware.os.amza.service.storage.binary;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.api.wal.RowIO;
import com.jivesoftware.os.amza.service.stats.IoStats;
import com.jivesoftware.os.amza.service.storage.filer.DiskBackedWALFiler;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class BinaryRowIOProvider implements RowIOProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final IoStats ioStats;
    private final OrderIdProvider orderIdProvider;
    private final int defaultUpdatesBetweenLeaps;
    private final int defaultMaxLeaps;
    private final boolean useMemMap;

    public BinaryRowIOProvider(
        IoStats ioStats,
        OrderIdProvider orderIdProvider,
        int defaultUpdatesBetweenLeaps,
        int defaultMaxLeaps,
        boolean useMemMap) {
        this.ioStats = ioStats;
        this.orderIdProvider = orderIdProvider;
        this.defaultUpdatesBetweenLeaps = defaultUpdatesBetweenLeaps;
        this.defaultMaxLeaps = defaultMaxLeaps;
        this.useMemMap = useMemMap;
    }

    @Override
    public RowIO open(File dir, String name, boolean createIfAbsent, int updatesBetweenLeaps, int maxLeaps) throws Exception {
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed trying to mkdirs for " + dir);
        }
        File file = new File(dir, name);
        if (!createIfAbsent && !file.exists()) {
            return null;
        }
        DiskBackedWALFiler filer = new DiskBackedWALFiler(file.getAbsolutePath(), "rw", useMemMap, 0);
        BinaryRowReader rowReader = new BinaryRowReader(filer, ioStats);
        BinaryRowWriter rowWriter = new BinaryRowWriter(filer, ioStats);
        return new BinaryRowIO(dir,
            name,
            rowReader,
            rowWriter,
            updatesBetweenLeaps > 0 ? updatesBetweenLeaps : defaultUpdatesBetweenLeaps,
            maxLeaps > 0 ? maxLeaps : defaultMaxLeaps);
    }

    @Override
    public List<String> listExisting(File dir) {
        File[] existing = dir.listFiles();
        return existing != null ? Lists.transform(Arrays.asList(existing), File::getName) : Collections.<String>emptyList();
    }

    @Override
    public File versionedKey(File baseKey, String latestVersion) throws Exception {
        File directory = new File(baseKey, latestVersion);
        FileUtils.forceMkdir(directory);
        return directory;
    }

    @Override
    public boolean ensureKey(File key) {
        return key.exists() || key.mkdirs();
    }

    @Override
    public boolean exists(File dir, String name) {
        return new File(dir, name).exists();
    }

    @Override
    public File buildKey(File versionedKey, String name) {
        return new File(versionedKey, name);
    }

    @Override
    public void moveTo(File fromDir, String fromName, File toDir, String toName) throws Exception {
        File from = new File(fromDir, fromName);
        File to = new File(toDir, toName);
        Files.move(from, to);
    }

    @Override
    public void safeMoveTo(File fromDir, String fromName, File toDir, String toName) throws Exception {
        File from = new File(fromDir, fromName);
        File to = new File(toDir, toName);

        while (to.getUsableSpace() < from.length()) {
            LOG.warn("Awaiting sufficient free space to move WAL from:{} to:{}", from, to);
            Thread.sleep(5_000); //TODO config
        }
        if (from.renameTo(to)) {
            LOG.info("Successfully renamed WAL from:{} to:{}", from, to);
        } else {
            LOG.warn("Unable to rename WAL from:{} to:{}, the file must be copied", from, to);
            File dest = new File(to.getParent(), orderIdProvider.nextId() + ".tmp");
            FileUtils.copyFile(from, dest);
            if (dest.renameTo(to)) {
                //TODO fsync dir
                LOG.info("Successfully copied WAL from:{} to:{}", from, to);
                FileUtils.deleteQuietly(from);
            } else {
                FileUtils.deleteQuietly(dest);
                throw new IOException("Failed to move copied WAL " +
                    " from:" + from +
                    " via:" + dest +
                    " to:" + to +
                    ", the file system does not appear to support this operation");
            }
        }
    }

    @Override
    public void delete(File dir, String name) throws Exception {
        FileUtils.deleteQuietly(new File(dir, name));
    }
}
