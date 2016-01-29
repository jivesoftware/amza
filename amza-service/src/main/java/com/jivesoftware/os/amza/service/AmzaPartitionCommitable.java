package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;

/**
 *
 */
public class AmzaPartitionCommitable implements Commitable {

    private final AmzaPartitionUpdates updates;

    public AmzaPartitionCommitable(AmzaPartitionUpdates updates) {
        this.updates = updates;
    }

    @Override
    public boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception {
        return updates.updates((key, value, valueTimestamp, valueTombstoned) -> {
            return txKeyValueStream.row(-1L, key, value, valueTimestamp, valueTombstoned, -1);
        });
    }
}
