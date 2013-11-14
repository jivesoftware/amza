package com.jivesoftware.os.amza.shared;

import java.util.Collection;

public interface TableRowWriter<R> {

    void write(Collection<R> rows, boolean append) throws Exception;
}