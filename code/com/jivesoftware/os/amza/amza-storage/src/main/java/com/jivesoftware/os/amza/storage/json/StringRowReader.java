/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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