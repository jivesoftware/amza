package com.jivesoftware.os.amza.api.wal;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public interface WALCompactionStats {

    Set<Map.Entry<String, Long>> getTimings();

    Set<Map.Entry<String, Long>> getCounts();

    void add(String name, long count);

    void start(String name);

    void stop(String name);
}
