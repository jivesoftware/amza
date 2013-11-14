package com.jivesoftware.os.amza.storage.json;

import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TableRowReader.Stream;
import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;

public class StringRowReader implements TableRowReader<String> {

    private final File file;

    public StringRowReader(File file) {
        this.file = file;
    }

    @Override
    public void read(boolean reverse, Stream<String> stream) throws Exception {
        if (file.exists()) {
            List<String> readLines = FileUtils.readLines(file, "utf-8");
            if (reverse) {
                Collections.reverse(readLines);
            }
            for (String line : readLines) {
                if (!stream.stream(line)) {
                    break;
                }
            }
        }
    }
}