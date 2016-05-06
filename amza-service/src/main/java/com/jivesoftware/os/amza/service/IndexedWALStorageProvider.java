package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.wal.PrimaryRowMarshaller;
import com.jivesoftware.os.amza.api.wal.WALIndex;
import com.jivesoftware.os.amza.api.wal.WALIndexProvider;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.amza.service.storage.WALStorage;
import com.jivesoftware.os.amza.service.storage.binary.BinaryHighwaterRowMarshaller;
import com.jivesoftware.os.amza.service.storage.binary.BinaryWALTx;
import com.jivesoftware.os.amza.service.storage.binary.RowIOProvider;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class IndexedWALStorageProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Random rand = new Random();
    private final AmzaStats amzaStats;
    private final File[] workingDirectories;
    private final int numberOfStripes;
    private final WALIndexProviderRegistry indexProviderRegistry;
    private final PrimaryRowMarshaller primaryRowMarshaller;
    private final BinaryHighwaterRowMarshaller highwaterRowMarshaller;
    private final TimestampedOrderIdProvider orderIdProvider;
    private final SickPartitions sickPartitions;
    private final int tombstoneCompactionFactor;
    private final long rebalanceIfImbalanceGreaterThanInBytes;

    public IndexedWALStorageProvider(AmzaStats amzaStats,
        File[] workingDirectories,
        int numberOfStripes,
        WALIndexProviderRegistry indexProviderRegistry,
        PrimaryRowMarshaller primaryRowMarshaller,
        BinaryHighwaterRowMarshaller highwaterRowMarshaller,
        TimestampedOrderIdProvider orderIdProvider,
        SickPartitions sickPartitions,
        int tombstoneCompactionFactor,
        long rebalanceIfImbalanceGreaterThanInBytes) throws IOException {
        this.amzaStats = amzaStats;

        this.workingDirectories = workingDirectories;
        this.numberOfStripes = numberOfStripes;
        this.indexProviderRegistry = indexProviderRegistry;
        this.primaryRowMarshaller = primaryRowMarshaller;
        this.highwaterRowMarshaller = highwaterRowMarshaller;
        this.orderIdProvider = orderIdProvider;
        this.sickPartitions = sickPartitions;
        this.tombstoneCompactionFactor = tombstoneCompactionFactor;
        this.rebalanceIfImbalanceGreaterThanInBytes = rebalanceIfImbalanceGreaterThanInBytes;

        for (File workingDirectory : workingDirectories) {
            if (workingDirectory.exists()) {
                for (File file : FileUtils.listFiles(workingDirectory, new String[] { "kvt" }, true)) {
                    File dest = convert(workingDirectory, file);
                    if (dest != null) {
                        if (!dest.getParentFile().mkdirs()) {
                            throw new IOException("Failed to mkdirs for " + dest);
                        }
                        if (!file.renameTo(dest)) {
                            throw new IOException("Failed to rename " + file + " to " + dest);
                        }
                        LOG.info("We repaired WAL from {} to {}", file, dest);
                    }
                }
            }
        }
    }

    private static File convert(File workingDirectory, File file) {
        String workingPath = workingDirectory.getAbsolutePath();
        String filename = file.getName();
        String partition = filename.substring(0, filename.indexOf('.'));
        try {
            long partitionVersion = Long.parseLong(partition); // 12345
            String trailingPath = file.getAbsolutePath().substring(workingPath.length() + 1); // 0/v15/backup/12345
            int firstSlash = trailingPath.indexOf('/'); // 1
            int currentModulo = Integer.parseInt(trailingPath.substring(0, firstSlash)); // 0
            String partitionPath = trailingPath.substring(firstSlash); // /v15/backup/12345
            long actualModulo = hash(partitionVersion) % 1024;
            if (currentModulo != actualModulo) {
                return new File(workingDirectory + "/" + actualModulo + partitionPath);
            } else {
                LOG.info("Skipped repair for WAL {}", partition);
            }
        } catch (NumberFormatException e) {
            LOG.info("Did not repair system WAL {}", partition);
        }
        return null;
    }

    public static void main(String[] args) {
        File from = new File("/example/barf/0/v15/backup/12345.kvt");
        File dest = convert(new File("/example/barf"), from);
        System.out.println(from.getAbsolutePath() + " to " + (dest == null ? "null" : dest.getAbsolutePath()));

        dest = convert(new File("/example/barf/"), from);
        System.out.println(from.getAbsolutePath() + " to " + (dest == null ? "null" : dest.getAbsolutePath()));

        from = new File("/example/barf/0/v15/backup/AAAAAAA==.kvt");
        dest = convert(new File("/example/barf/"), from);
        System.out.println(from.getAbsolutePath() + " to " + (dest == null ? "null" : dest.getAbsolutePath()));

        int[] count = new int[1024];
        for (int i = 0; i < 10_000; i += 2) {
            int h = (int) (hash((long) i) % 1024);
            count[h]++;
        }
        for (int i = 0; i < 1024; i++) {
            System.out.println(i + ". " + count[i]);
        }
    }

    private String name(VersionedPartitionName versionedPartitionName) throws IOException {
        return (versionedPartitionName.getPartitionVersion() == VersionedPartitionName.STATIC_VERSION)
            ? versionedPartitionName.toBase64()
            : String.valueOf(versionedPartitionName.getPartitionVersion());
    }

    public int rebalanceToStripe(VersionedPartitionName versionedPartitionName, int stripe, PartitionProperties partitionProperties) throws Exception {
        int numberOfWorkingDirectories = workingDirectories.length;
        long[] freeSpace = new long[numberOfWorkingDirectories];
        long maxFree = Long.MIN_VALUE;
        int maxFreeIndex = -1;

        long minFree = Long.MAX_VALUE;
        int minFreeIndex = -1;
        for (int i = 0; i < numberOfWorkingDirectories; i++) {
            freeSpace[i] = workingDirectories[i].getFreeSpace();
            if (freeSpace[i] < minFree) {
                minFree = freeSpace[i];
                minFreeIndex = i;
            }
            if (freeSpace[i] > maxFree) {
                maxFree = freeSpace[i];
                maxFreeIndex = i;
            }
        }

        long imbalance = freeSpace[maxFreeIndex] - freeSpace[minFreeIndex];

        if (imbalance > rebalanceIfImbalanceGreaterThanInBytes) {
            int maxStripeCount = (numberOfStripes / numberOfWorkingDirectories) + (maxFreeIndex < (numberOfStripes % numberOfWorkingDirectories) ? 1 : 0);
            int rebalanceToStripe = maxFreeIndex + (numberOfWorkingDirectories * rand.nextInt(maxStripeCount));

            if (stripe % numberOfWorkingDirectories == minFreeIndex && rebalanceToStripe != stripe) {
                String providerName = partitionProperties.indexClassName;
                @SuppressWarnings("unchecked")
                RowIOProvider rowIOProvider = indexProviderRegistry.getRowIOProvider(providerName);

                long sizeOfWAL = BinaryWALTx.sizeInBytes(baseKey(versionedPartitionName, stripe), name(versionedPartitionName), rowIOProvider);
                if (sizeOfWAL * 2 < rebalanceIfImbalanceGreaterThanInBytes) { // the times 2 says our index shouldn't be any bigger than our wal ;)
                    return rebalanceToStripe;
                }
            }
        }
        return -1;
    }

    public File baseKey(VersionedPartitionName versionedPartitionName, int stripe) {
        return new File(workingDirectories[stripe % workingDirectories.length], String.valueOf(hash(versionedPartitionName) % 1024));
    }

    private final static long randMult = 0x5DEECE66DL;
    private final static long randAdd = 0xBL;
    private final static long randMask = (1L << 48) - 1;

    private static long hash(VersionedPartitionName versionedPartitionName) {
        if (versionedPartitionName.getPartitionName().isSystemPartition()) {
            return versionedPartitionName.getPartitionVersion();
        } else {
            return hash(versionedPartitionName.getPartitionVersion());
        }
    }

    private static long hash(long partitionVersion) {
        long x = (partitionVersion * randMult + randAdd) & randMask;
        long h = Math.abs(x >>> (16));
        if (h >= 0) {
            return h;
        } else {
            return Long.MAX_VALUE;
        }
    }

    public <I extends WALIndex> WALStorage<I> create(
        VersionedPartitionName versionedPartitionName,
        PartitionProperties partitionProperties) throws Exception {

        String providerName = partitionProperties.indexClassName;
        @SuppressWarnings("unchecked")
        WALIndexProvider<I> walIndexProvider = (WALIndexProvider<I>) indexProviderRegistry.getWALIndexProvider(providerName);
        @SuppressWarnings("unchecked")
        RowIOProvider rowIOProvider = indexProviderRegistry.getRowIOProvider(providerName);

        String name = name(versionedPartitionName);

        BinaryWALTx binaryWALTx = new BinaryWALTx(
            name,
            rowIOProvider,
            primaryRowMarshaller,
            partitionProperties.updatesBetweenLeaps,
            partitionProperties.maxLeaps);
        boolean hardFsyncBeforeLeapBoundary = versionedPartitionName.getPartitionName().isSystemPartition();
        return new WALStorage<>(amzaStats,
            versionedPartitionName,
            orderIdProvider,
            primaryRowMarshaller,
            highwaterRowMarshaller,
            binaryWALTx,
            walIndexProvider,
            sickPartitions,
            hardFsyncBeforeLeapBoundary,
            tombstoneCompactionFactor);
    }
}
