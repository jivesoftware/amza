package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.api.AmzaVersionConstants;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.lab.pointers.LABPointerIndexWALIndexName.Type;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 *
 */
public class LABPointerIndexWALIndexProvider implements WALIndexProvider<LABPointerIndexWALIndex> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    public static final String INDEX_CLASS_NAME = "lab";

    private final String name;
    private final LABEnvironment[] environments;
    private final LABPointerIndexConfig config;
    private final LRUConcurrentBAHLinkedHash<Leaps> leapCache;

    public LABPointerIndexWALIndexProvider(LABPointerIndexConfig config,
        String name,
        int numberOfStripes,
        File[] baseDirs) throws IOException {

        this.config = config;
        this.name = name;
        this.environments = new LABEnvironment[numberOfStripes];

        ExecutorService compactorThreadPool = LABEnvironment.buildLABCompactorThreadPool(config.getConcurrency());
        ExecutorService destroyThreadPool = LABEnvironment.buildLABDestroyThreadPool(environments.length);
        this.leapCache = LABEnvironment.buildLeapsCache((int) config.getLeapCacheMaxCapacity(), config.getConcurrency());

        for (int i = 0; i < environments.length; i++) {
            File active = new File(
                new File(
                    new File(baseDirs[i % baseDirs.length], AmzaVersionConstants.LATEST_VERSION),
                    INDEX_CLASS_NAME),
                String.valueOf(i));
            if (!active.exists() && !active.mkdirs()) {
                throw new RuntimeException("Failed while trying to mkdirs for " + active);
            }
            this.environments[i] = new LABEnvironment(compactorThreadPool,
                destroyThreadPool,
                active,
                config.getUseMemMap(),
                config.getMinMergeDebt(),
                config.getMaxMergeDebt(),
                leapCache);

            File[] files = active.listFiles();
            if (files != null) {
                BAInterner interner = new BAInterner();
                for (File file : files) {
                    if (file.isDirectory()) {
                        File dest = convertBase64toPartitionVersion(file, interner);
                        if (dest != null && !file.renameTo(dest)) {
                            throw new IOException("Failed rename of " + file + " to " + dest);
                        }
                    }
                }
            }

            files = active.listFiles();
            if (files != null) {
                for (File file : files) {
                    String[] split = file.getName().split("-");
                    if (split.length == 3) {
                        try {
                            long partitionVersion = Long.parseLong(split[2]);
                            long h = hash(partitionVersion);

                            File parent = new File(active, String.valueOf(h % 1024));
                            if (!parent.mkdirs()) {
                                throw new IOException("Failed to mkdirs for " + parent);
                            }

                            File dest = new File(parent, file.getName());
                            if (!file.renameTo(dest)) {
                                throw new IOException("Failed to move " + file + " to " + dest);
                            }

                            LOG.info("We hash repaired {} to {}", file, dest);
                        } catch (NumberFormatException e) {
                            LOG.info("Skipped repair for " + file);
                        }
                    }
                }
            }
        }
    }

    private final static long randMult = 0x5DEECE66DL;
    private final static long randAdd = 0xBL;
    private final static long randMask = (1L << 48) - 1;

    private static long hash(long partitionVersion) {
        long x = (partitionVersion * randMult + randAdd) & randMask;
        long h = Math.abs(x >>> (16));
        if (h >= 0) {
            return h;
        } else {
            return Long.MAX_VALUE;
        }
    }

    private static File convertBase64toPartitionVersion(File file, BAInterner interner) throws IOException {

        String filename = file.getName();
        int firstHyphen = filename.indexOf('-');
        int secondHyphen = filename.indexOf('-', firstHyphen + 1);
        if (firstHyphen != -1 && secondHyphen != -1) {
            String base = filename.substring(0, secondHyphen);
            String partition = filename.substring(secondHyphen + 1);
            try {
                long partitionVersion = Long.parseLong(partition);
                LOG.info("Did not repair partition version {}", partitionVersion);
            } catch (NumberFormatException e) {
                VersionedPartitionName vpn = VersionedPartitionName.fromBase64(partition, interner);
                LOG.info("We will repair partition {}", vpn);
                return new File(file.getParent(), base + "-" + String.valueOf(vpn.getPartitionVersion()));
            }
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(convertBase64toPartitionVersion(
            new File("/example/prefix-active-" + new VersionedPartitionName(new PartitionName(false, "abc".getBytes(), "def".getBytes()), 123L).toBase64()),
            new BAInterner()));
        System.out.println(convertBase64toPartitionVersion(
            new File("/example/prefix-active-123"),
            new BAInterner()));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public LABPointerIndexWALIndex createIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        int modulo = (int) (hash(versionedPartitionName.getPartitionVersion()) % 1024);
        LABPointerIndexWALIndexName indexName = new LABPointerIndexWALIndexName(modulo,
            Type.active,
            String.valueOf(versionedPartitionName.getPartitionVersion()));
        //TODO config flush interval
        return new LABPointerIndexWALIndex(name,
            versionedPartitionName,
            environments,
            stripe,
            indexName,
            config);
    }

    @Override
    public void deleteIndex(VersionedPartitionName versionedPartitionName, int stripe) throws Exception {
        int modulo = (int) (hash(versionedPartitionName.getPartitionVersion()) % 1024);
        LABPointerIndexWALIndexName name = new LABPointerIndexWALIndexName(modulo,
            LABPointerIndexWALIndexName.Type.active,
            String.valueOf(versionedPartitionName.getPartitionVersion()));
        LABEnvironment env = environments[stripe];
        for (LABPointerIndexWALIndexName n : name.all()) {
            env.remove(n.getPrimaryName());
            LOG.info("Removed database: {}", n.getPrimaryName());
            env.remove(n.getPrefixName());
            LOG.info("Removed database: {}", n.getPrefixName());
        }
    }

    @Override
    public void flush(Iterable<LABPointerIndexWALIndex> indexes, boolean fsync) throws Exception {
        for (LABPointerIndexWALIndex index : indexes) {
            index.flush(fsync); // So call me maybe?
        }
    }

}
