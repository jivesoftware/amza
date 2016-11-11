package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import java.util.List;

/**
 *
 */
public interface AmzaSyncClient {
    void commitRows(PartitionName toPartitionName, List<Row> rows) throws Exception;

    void ensurePartition(PartitionName toPartitionName, PartitionProperties properties) throws Exception;
}
