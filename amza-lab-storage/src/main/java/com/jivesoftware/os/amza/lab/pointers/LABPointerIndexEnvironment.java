package com.jivesoftware.os.amza.lab.pointers;

import com.jivesoftware.os.amza.lab.pointers.api.PointerIndex;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class LABPointerIndexEnvironment {

    private final File rootFile;
    private final ExecutorService merge = Executors.newCachedThreadPool(); // TODO config 'maybe'
    private final ExecutorService destroy = Executors.newSingleThreadExecutor(); // TODO config 'maybe'
    private final int maxMergeDebt;

    public LABPointerIndexEnvironment(File rootFile, int maxMergeDebt) {
        this.rootFile = rootFile;
        this.maxMergeDebt = maxMergeDebt;
    }

    PointerIndex open(String primaryName, int maxUpdatesBetweenCompactionHintMarker) throws Exception {
        File indexRoot = new File(rootFile, primaryName + File.separator);
        ensure(indexRoot);
        return new LABPointerIndex(merge, destroy, indexRoot, maxUpdatesBetweenCompactionHintMarker, maxMergeDebt);
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
}
