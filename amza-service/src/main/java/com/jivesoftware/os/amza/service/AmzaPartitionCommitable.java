package com.jivesoftware.os.amza.service;

import com.jivesoftware.os.amza.api.stream.Commitable;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;

/**
 *
 */
public class AmzaPartitionCommitable implements Commitable {

    private final AmzaPartitionUpdates updates;
    private final OrderIdProvider orderIdProvider;

    public AmzaPartitionCommitable(AmzaPartitionUpdates updates, OrderIdProvider orderIdProvider) {
        this.updates = updates;
        this.orderIdProvider = orderIdProvider;
    }

    @Override
    public boolean commitable(Highwaters highwaters, UnprefixedTxKeyValueStream txKeyValueStream) throws Exception {
        long version = orderIdProvider.nextId();
        return updates.updates((key, value, valueTimestamp, valueTombstoned) -> {
            return txKeyValueStream.row(-1L, key, value, valueTimestamp, valueTombstoned, version);
        });
    }
}
