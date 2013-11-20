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
package com.jivesoftware.os.amza.transport.http.replication.endpoints;

import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TableRowReader.Stream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StringArrayRowReader implements TableRowReader<String> {

    private final List<String> rows;

    public StringArrayRowReader(List<String> rows) {
        this.rows = rows;
    }

    @Override
    public void read(boolean reverse, Stream<String> stream) throws Exception {
        List<String> stackCopy = rows;
        if (reverse) {
            stackCopy = new ArrayList<>(rows);
            Collections.reverse(stackCopy);
        }
        for (String line : stackCopy) {
            if (!stream.stream(line)) {
                break;
            }
        }
    }
}