package com.jivesoftware.os.amza.storage;

import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.SecondaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.WALIndex;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALPointer;
import com.jivesoftware.os.filer.io.AutoGrowingByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.FileBackedMemMappedByteBufferFactory;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.map.MapStore;
import com.jivesoftware.os.filer.io.map.SkipListMapContext;
import com.jivesoftware.os.filer.io.map.SkipListMapStore;
import com.jivesoftware.os.filer.map.store.LexSkipListComparator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;

/**
 * @author jonathan.colt
 */
public class FileBackedWALIndex implements WALIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RegionName regionName;
    private final int keySize;
    private final boolean variableKeySizes;
    private final int payloadSize = 8 + 8 + 1; // fp, timetamp, tombstone
    private final int directoryOffset;
    private final File[] directories;
    private AutoGrowingByteBufferBackedFiler filer;
    private SkipListMapContext sls;

    public FileBackedWALIndex(RegionName regionName,
        int keySize,
        boolean variableKeySizes,
        int directoryOffset,
        File... directories) {
        this.regionName = regionName;
        this.keySize = keySize;
        this.variableKeySizes = variableKeySizes;
        this.directoryOffset = directoryOffset;
        this.directories = directories;
    }

    synchronized SkipListMapContext ensureCapacity(long roomForNMore) throws IOException {
        if (roomForNMore == 0 && filer == null) {
            return null;
        }

        if (filer == null) {
            String prefix = regionName.toBase64();
            byte[] headKey = new byte[keySize];
            Arrays.fill(headKey, Byte.MIN_VALUE);

            boolean compactedExists = false;
            for (File directory : directories) {
                File compactedDirectory = new File(directory, prefix + "/compacted");
                if (compactedDirectory.exists()) {
                    compactedExists = true;
                    break;
                }
            }

            if (compactedExists) {
                for (File directory : directories) {
                    File compactingDirectory = new File(directory, prefix + "/compacting/active");
                    if (compactingDirectory.exists()) {
                        FileUtils.moveDirectory(compactingDirectory, directory);
                    }
                    File compactedDirectory = new File(directory, prefix + "/compacted");
                    if (compactedDirectory.exists()) {
                        FileUtils.moveDirectory(compactedDirectory, directory);
                    }
                }
            }

            boolean coldStart = true;
            boolean fallbackToBackup = false;
            for (File directory : directories) {
                File activeDirectory = new File(directory, prefix + "/active");
                if (!activeDirectory.exists()) {
                    File newDirectory = new File(directory, prefix + "/growing");
                    if (newDirectory.exists()) {
                        FileUtils.moveDirectory(newDirectory, directory);
                        coldStart = false;
                    } else {
                        fallbackToBackup = true;
                        break;
                    }
                } else {
                    coldStart = false;
                }
            }

            if (fallbackToBackup) {
                int count = 0;
                for (File directory : directories) {
                    File backupDirectory = new File(directory, prefix + "/bkp");
                    if (backupDirectory.exists()) {
                        count++;
                    }
                }
                if (count == 0) {
                    coldStart = true;
                } else if (count == directories.length) {
                    for (File directory : directories) {
                        File backupDirectory = new File(directory, prefix + "/bkp");
                        File activeDirectory = new File(directory, prefix + "/active");
                        FileUtils.moveDirectory(backupDirectory, activeDirectory);
                    }
                    coldStart = false;
                } else {
                    LOG.error("Corrupt rowIndex for {}. Please rebuild.", regionName);
                    throw new RuntimeException("Your row index is corrupt. Please rebuild." + regionName);
                }
            }

            if (coldStart) {
                File[] activeDirectories = new File[directories.length];
                for (int i = 0; i < directories.length; i++) {
                    activeDirectories[i] = new File(directories[i], prefix + "/active");
                    activeDirectories[i].mkdirs();
                }

                long size = SkipListMapStore.INSTANCE.computeFilerSize((int) (2 + roomForNMore), keySize, variableKeySizes, payloadSize, (byte) 9);
                FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(prefix, directoryOffset, activeDirectories);
                AutoGrowingByteBufferBackedFiler coldStartFiler = new AutoGrowingByteBufferBackedFiler(factory, 1024,
                    AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);

                SkipListMapContext slmc = SkipListMapStore.INSTANCE.create((int) (2 + roomForNMore), headKey, keySize, variableKeySizes, payloadSize, (byte) 9,
                    LexSkipListComparator.cSingleton, coldStartFiler);
                coldStartFiler.seek(size);
                coldStartFiler.seek(0);

                filer = coldStartFiler;
                sls = slmc;

            } else {
                File[] activeDirectories = new File[directories.length];
                for (int i = 0; i < directories.length; i++) {
                    activeDirectories[i] = new File(directories[i], prefix + "/active");
                }

                FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(prefix, directoryOffset, activeDirectories);
                AutoGrowingByteBufferBackedFiler openFiler = new AutoGrowingByteBufferBackedFiler(factory, 1024,
                    AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);

                SkipListMapContext slmc = SkipListMapStore.INSTANCE.open(headKey, LexSkipListComparator.cSingleton, openFiler);
                long size = SkipListMapStore.INSTANCE.computeFilerSize(slmc.mapContext.maxCount, keySize, variableKeySizes, payloadSize, (byte) 9);
                openFiler.seek(size);
                openFiler.seek(0);

                filer = openFiler;
                sls = slmc;
            }
        } else {
            if (MapStore.INSTANCE.isFull(sls.mapContext)) {
                String prefix = regionName.toBase64();
                byte[] headKey = new byte[keySize];
                Arrays.fill(headKey, Byte.MIN_VALUE);

                int nextGrowSize = MapStore.INSTANCE.nextGrowSize(sls.mapContext, (int) roomForNMore);
                long newSize = SkipListMapStore.INSTANCE.computeFilerSize(nextGrowSize, keySize, variableKeySizes, payloadSize, (byte) 9);

                File[] activeDirectories = new File[directories.length];
                File[] backupDirectories = new File[directories.length];
                File[] newDirectories = new File[directories.length];
                for (int i = 0; i < directories.length; i++) {
                    activeDirectories[i] = new File(directories[i], prefix + "/active");
                    backupDirectories[i] = new File(directories[i], prefix + "/bkp");
                    newDirectories[i] = new File(directories[i], prefix + "/growing");
                }

                for (int i = 0; i < directories.length; i++) {
                    FileUtils.deleteDirectory(newDirectories[i]);
                    newDirectories[i].mkdirs();
                }

                FileBackedMemMappedByteBufferFactory newFactory = new FileBackedMemMappedByteBufferFactory(prefix, directoryOffset, newDirectories);
                AutoGrowingByteBufferBackedFiler newFiler = new AutoGrowingByteBufferBackedFiler(
                    newFactory, newSize, AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);

                try {
                    SkipListMapContext newSLS = SkipListMapStore.INSTANCE.create(nextGrowSize, headKey, keySize, variableKeySizes, payloadSize, (byte) 9,
                        LexSkipListComparator.cSingleton, newFiler);
                    SkipListMapStore.INSTANCE.copyTo(filer, sls, newFiler, newSLS, null);

                } catch (Exception x) {
                    LOG.error("Failed to grow row index for " + regionName, x);
                    try {
                        for (File dir : newDirectories) {
                            FileUtils.deleteDirectory(dir);
                        }
                    } catch (Exception xx) {
                        LOG.error("Failed to cleanup after a failed grow " + regionName, xx);
                        throw xx;
                    }
                    throw x;
                }
                filer.close();
                filer = null;
                sls = null;
                newFiler.close();

                try {
                    for (int i = 0; i < directories.length; i++) {
                        FileUtils.moveDirectory(activeDirectories[i], backupDirectories[i]);
                    }

                    for (int i = 0; i < directories.length; i++) {
                        FileUtils.moveDirectory(newDirectories[i], activeDirectories[i]);
                    }

                    for (int i = 0; i < directories.length; i++) {
                        FileUtils.deleteDirectory(backupDirectories[i]);
                    }
                } catch (Exception x) {
                    LOG.error("Failed to suffle the dirs after a grow.", x);
                    throw x;
                }

                FileBackedMemMappedByteBufferFactory factory = new FileBackedMemMappedByteBufferFactory(prefix, directoryOffset, activeDirectories);
                AutoGrowingByteBufferBackedFiler reOpenFiler = new AutoGrowingByteBufferBackedFiler(factory, 1024,
                    AutoGrowingByteBufferBackedFiler.MAX_BUFFER_SEGMENT_SIZE);
                SkipListMapContext slmc = SkipListMapStore.INSTANCE.open(headKey, LexSkipListComparator.cSingleton, reOpenFiler);
                long size = SkipListMapStore.INSTANCE.computeFilerSize(slmc.mapContext.maxCount, keySize, variableKeySizes, payloadSize, (byte) 9);
                reOpenFiler.seek(size);
                reOpenFiler.seek(0);

                filer = reOpenFiler;
                sls = slmc;
            }
        }
        return sls;

    }

    byte[] ser(WALPointer value) {
        ByteBuffer bb = ByteBuffer.allocate(payloadSize);
        bb.putLong(value.getFp());
        bb.putLong(value.getTimestampId());
        bb.put(value.getTombstoned() ? (byte) 1 : 0);

        return bb.array();
    }

    WALPointer der(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value);
        return new WALPointer(bb.getLong(), bb.getLong(), bb.get() == 1 ? true : false);
    }

    @Override
    synchronized public void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entries) {
        try {
            SkipListMapContext slmc = ensureCapacity(entries.size());
            for (Map.Entry<WALKey, WALPointer> entry : entries) {
                SkipListMapStore.INSTANCE.add(filer, slmc, entry.getKey().getKey(), ser(entry.getValue()));
            }
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    public WALPointer getPointer(WALKey key) throws Exception {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            if (slmc != null) {
                byte[] got = SkipListMapStore.INSTANCE.getExistingPayload(filer, sls, key.getKey());
                if (got != null) {
                    return der(got);
                }
            }
            return null;
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public WALPointer[] getPointers(WALKey[] consumableKeys) throws Exception {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            WALPointer[] gots = new WALPointer[consumableKeys.length];
            if (slmc != null) {
                for (int i = 0; i < consumableKeys.length; i++) {
                    WALKey key = consumableKeys[i];
                    if (key != null) {
                        byte[] got = SkipListMapStore.INSTANCE.getExistingPayload(filer, sls, key.getKey());
                        if (got != null) {
                            gots[i] = der(got);
                            consumableKeys[i] = null;
                        }
                    }
                }
            }
            return gots;

        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public List<Boolean> containsKey(List<WALKey> keys) {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            List<Boolean> contains = new ArrayList<>(keys.size());
            if (slmc == null) {
                contains.addAll(Collections.<Boolean>nCopies(keys.size(), Boolean.FALSE));
            } else {
                for (WALKey key : keys) {
                    WALPointer pointer = getPointer(key);
                    contains.add(pointer == null ? false : !pointer.getTombstoned());
                }
            }
            return contains;
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public void remove(Collection<WALKey> keys) {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            if (slmc != null) {
                for (WALKey key : keys) {
                    SkipListMapStore.INSTANCE.remove(filer, sls, key.getKey());
                }
            }
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public boolean isEmpty() {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            if (slmc != null) {
                return MapStore.INSTANCE.getCount(filer) == 0;
            } else {
                return true;
            }
        } catch (Exception x) {
            throw new RuntimeException("Failure while putting.", x);
        }
    }

    @Override
    synchronized public void commit() {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    synchronized public void compact() {

    }

    @Override
    synchronized public void rowScan(final Scan<WALPointer> scan) throws Exception {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            if (slmc != null) {
                SkipListMapStore.INSTANCE.streamKeys(filer, slmc, slmc, null, (byte[] key) -> {
                    byte[] got = SkipListMapStore.INSTANCE.getExistingPayload(filer, sls, key);
                    if (got != null) {
                        WALPointer value = der(got);
                        try {
                            return scan.row(-1, new WALKey(key), value);
                        } catch (Exception e) {
                            throw new RuntimeException("Error in rowScan.", e);
                        }
                    }
                    return true;
                });
            }
        } catch (Exception x) {
            throw new RuntimeException("streamKeys failure:", x);
        }
    }

    @Override
    synchronized public void rangeScan(WALKey from, WALKey to, final Scan<WALPointer> scan) throws Exception {
        try {
            List<KeyRange> ranges = Collections.singletonList(new KeyRange(from.getKey(), to.getKey()));
            SkipListMapContext slmc = ensureCapacity(0);
            if (slmc != null) {
                SkipListMapStore.INSTANCE.streamKeys(filer, sls, sls, ranges, (byte[] key) -> {
                    byte[] got = SkipListMapStore.INSTANCE.getExistingPayload(filer, sls, key);
                    if (got != null) {
                        WALPointer value = der(got);
                        try {
                            return scan.row(-1, new WALKey(key), value);
                        } catch (Exception e) {
                            throw new RuntimeException("Error in rangeScan.", e);
                        }
                    }
                    return true;
                });
            }
        } catch (Exception x) {
            throw new RuntimeException("streamKeys failure:", x);
        }
    }

    @Override
    synchronized public long size() {
        try {
            SkipListMapContext slmc = ensureCapacity(0);
            if (slmc != null) {
                return SkipListMapStore.INSTANCE.getCount(filer, sls);
            } else {
                return 0;
            }
        } catch (Exception x) {
            throw new RuntimeException("Failure while computing size.", x);
        }
    }

    @Override
    public CompactionWALIndex startCompaction() throws Exception {
        final String prefix = regionName.toBase64();
        final File[] compactingDirectories = new File[directories.length];
        final File[] compactedDirectories = new File[directories.length];
        for (int i = 0; i < directories.length; i++) {
            compactedDirectories[i] = new File(directories[i], prefix + "/compacted");
            if (compactedDirectories[i].exists()) {
                FileUtils.deleteDirectory(compactedDirectories[i]);
            }
            compactingDirectories[i] = new File(directories[i], prefix + "/compacting");
            if (compactingDirectories[i].exists()) {
                FileUtils.deleteDirectory(compactingDirectories[i]);
            }
            compactingDirectories[i].mkdirs();
        }

        final FileBackedWALIndex fileBackedRowIndex = new FileBackedWALIndex(regionName,
            keySize,
            variableKeySizes,
            directoryOffset,
            compactingDirectories
        );

        synchronized (this) {
            if (filer != null) {
                fileBackedRowIndex.ensureCapacity(MapStore.INSTANCE.getCount(filer));
            }
        }

        return new CompactionWALIndex() {

            @Override
            public void put(Collection<? extends Map.Entry<WALKey, WALPointer>> entries) {
                fileBackedRowIndex.put(entries);
            }

            @Override
            public void abort() {
                for (File compactingDirectory : compactingDirectories) {
                    try {
                        FileUtils.deleteDirectory(compactingDirectory);
                    } catch (Exception x) {
                        LOG.error("Failed to remove compactingDirectories:" + compactingDirectory);
                    }
                }
            }

            @Override
            public void commit() throws Exception {
                synchronized (FileBackedWALIndex.this) {

                    if (filer != null) {
                        filer.close();
                        filer = null;
                        sls = null;
                    }

                    for (File directory : directories) {
                        File active = new File(directory, prefix + "/active");
                        FileUtils.deleteDirectory(active);
                    }

                    for (int i = 0; i < directories.length; i++) {
                        File active = new File(compactingDirectories[i], prefix + "/active");
                        FileUtils.moveDirectory(active, compactedDirectories[i]);
                    }

                    for (int i = 0; i < directories.length; i++) {
                        File active = new File(directories[i], prefix + "/active");
                        FileUtils.moveDirectory(compactedDirectories[i], active);
                    }

                    ensureCapacity(1);

                }
            }
        };
    }

    @Override
    public void updatedDescriptors(PrimaryIndexDescriptor primaryIndexDescriptor, SecondaryIndexDescriptor[] secondaryIndexDescriptors) {
    }

    @Override
    public boolean delete() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
