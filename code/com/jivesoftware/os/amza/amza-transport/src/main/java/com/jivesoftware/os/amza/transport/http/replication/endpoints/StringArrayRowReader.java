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