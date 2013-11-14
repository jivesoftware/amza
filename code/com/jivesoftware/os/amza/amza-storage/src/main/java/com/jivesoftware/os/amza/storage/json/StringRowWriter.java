package com.jivesoftware.os.amza.storage.json;

import com.jivesoftware.os.amza.shared.TableRowWriter;
import java.io.File;
import java.util.Collection;
import org.apache.commons.io.FileUtils;

public class StringRowWriter implements TableRowWriter<String> {

    private final File file;

    public StringRowWriter(File file) {
        this.file = file;
    }

    @Override
    public void write(Collection<String> rows, boolean append) throws Exception {
        file.getParentFile().mkdirs();
        FileUtils.writeLines(file, "utf-8", rows, "\n", append);
    }
}