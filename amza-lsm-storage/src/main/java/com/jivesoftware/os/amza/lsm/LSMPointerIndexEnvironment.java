/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.lsm;

import com.google.common.io.Files;
import com.jivesoftware.os.amza.lsm.api.ConcurrentReadablePointerIndex;
import com.jivesoftware.os.amza.lsm.api.NextPointer;
import com.jivesoftware.os.amza.lsm.api.Pointers;
import java.io.File;
import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author jonathan.colt
 */
public class LSMPointerIndexEnvironment {

    private final File rootFile;

    public LSMPointerIndexEnvironment(File rootFile) {
        this.rootFile = rootFile;
    }

    PointerIndex open(String primaryName) throws IOException {
        File indexRoot = new File(rootFile, primaryName + File.separator);
        ensure(indexRoot);
        return new LSMPointerIndex(indexRoot);
    }

    boolean ensure(File key) {
        return key.exists() || key.mkdirs();
    }

    void rename(String oldName, String newName) throws IOException {
        File oldFileName = new File(rootFile, oldName + File.separator);
        File newFileName = new File(rootFile, newName + File.separator);
        FileUtils.moveDirectory(oldFileName, newFileName);
        FileUtils.deleteDirectory(oldFileName);
    }

    void remove(String primaryName) throws IOException {
        File fileName = new File(rootFile, primaryName + File.separator);
        FileUtils.deleteDirectory(fileName);
    }

    void flushLog(boolean b) {
    }

    public static class LSMPointerIndex implements PointerIndex {

        private final File indexRoot;
        private MemoryPointerIndex memoryPointerIndex;
        private MemoryPointerIndex flushingMemoryPointerIndex;
        private final AtomicLong largestIndexId = new AtomicLong();
        private final MergeablePointerIndexs mergeablePointerIndexs;

        public LSMPointerIndex(File indexRoot) throws IOException {
            this.indexRoot = indexRoot;
            this.memoryPointerIndex = new MemoryPointerIndex();
            this.mergeablePointerIndexs = new MergeablePointerIndexs();

            TreeSet<Long> indexIds = new TreeSet<>((o1, o2) -> Long.compare(o2, o1)); // descending
            for (File indexFile : indexRoot.listFiles()) {
                long indexId = Long.parseLong(FilenameUtils.removeExtension(indexFile.getName()));
                indexIds.add(indexId);
                if (largestIndexId.get() < indexId) {
                    largestIndexId.set(indexId);
                }
            }

            for (Long indexId : indexIds) {
                File index = new File(indexRoot, String.valueOf(indexId));
                DiskBackedPointerIndex pointerIndex = new DiskBackedPointerIndex(
                    new DiskBackedPointerIndexFiler(new File(index, "index").getAbsolutePath(), "rw", false),
                    new DiskBackedPointerIndexFiler(new File(index, "keys").getAbsolutePath(), "rw", false));
                mergeablePointerIndexs.append(pointerIndex);
            }
        }

        private ConcurrentReadablePointerIndex[] grab() {
            MemoryPointerIndex stackCopy = flushingMemoryPointerIndex;
            int flushing = stackCopy == null ? 0 : 1;

            ConcurrentReadablePointerIndex[] grabbed = mergeablePointerIndexs.grab();
            ConcurrentReadablePointerIndex[] indexes = new ConcurrentReadablePointerIndex[grabbed.length + 1 + flushing];
            synchronized (this) {
                indexes[0] = memoryPointerIndex;
                if (stackCopy != null) {
                    indexes[1] = stackCopy;
                }
            }
            System.arraycopy(grabbed, 0, indexes, 1 + flushing, grabbed.length);
            return indexes;
        }

        @Override
        public NextPointer getPointer(byte[] key) throws Exception {
            return PointerIndexUtil.get(grab(), key);
        }

        @Override
        public NextPointer rangeScan(byte[] from, byte[] to) throws Exception {
            return PointerIndexUtil.rangeScan(grab(), from, to);
        }

        @Override
        public NextPointer rowScan() throws Exception {
            return PointerIndexUtil.rowScan(grab());
        }

        @Override
        public void close() throws Exception {
            memoryPointerIndex.close();
            mergeablePointerIndexs.close();
        }

        @Override
        public long count() throws Exception {
            return memoryPointerIndex.count() + mergeablePointerIndexs.count();
        }

        @Override
        public boolean isEmpty() throws Exception {
            if (memoryPointerIndex.isEmpty()) {
                return mergeablePointerIndexs.isEmpty();
            }
            return false;
        }

        @Override
        public void append(Pointers pointers) throws Exception {
            memoryPointerIndex.append(pointers);
        }

        @Override
        public void flush() throws Exception {
            synchronized (this) { // TODO use semaphores instead of a hard lock
                if (flushingMemoryPointerIndex != null) {
                    throw new RuntimeException("Concurrently trying to flush while a flush is underway.");
                }
                flushingMemoryPointerIndex = memoryPointerIndex;
                memoryPointerIndex = new MemoryPointerIndex();
            }
            long nextIndexId = largestIndexId.incrementAndGet();
            File tmpRoot = Files.createTempDir();
            DiskBackedPointerIndex pointerIndex = new DiskBackedPointerIndex(
                new DiskBackedPointerIndexFiler(new File(tmpRoot, "index").getAbsolutePath(), "rw", false),
                new DiskBackedPointerIndexFiler(new File(tmpRoot, "keys").getAbsolutePath(), "rw", false));
            pointerIndex.append(flushingMemoryPointerIndex);
            pointerIndex.close();

            File index = new File(indexRoot, Long.toString(nextIndexId));
            FileUtils.moveDirectory(tmpRoot, index);

            pointerIndex = new DiskBackedPointerIndex(
                new DiskBackedPointerIndexFiler(new File(index, "index").getAbsolutePath(), "rw", false),
                new DiskBackedPointerIndexFiler(new File(index, "keys").getAbsolutePath(), "rw", false));

            synchronized (this) {
                mergeablePointerIndexs.append(pointerIndex);
                flushingMemoryPointerIndex = null;
            }
        }

        @Override
        public String toString() {
            return "LSMPointerIndex{"
                + "indexRoot=" + indexRoot
                + ", memoryPointerIndex=" + memoryPointerIndex
                + ", flushingMemoryPointerIndex=" + flushingMemoryPointerIndex
                + ", largestIndexId=" + largestIndexId
                + ", mergeablePointerIndexs=" + mergeablePointerIndexs
                + '}';
        }

    }
}
