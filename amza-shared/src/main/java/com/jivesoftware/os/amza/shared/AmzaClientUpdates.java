package com.jivesoftware.os.amza.shared;

import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.UnprefixedTxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;

/**
 *
 */
public class AmzaClientUpdates implements ClientUpdates {

    private static final Highwaters NO_OP_HIGHWATERS = highwater -> {
    };

    private final AmzaPartitionUpdates updates;

    public AmzaClientUpdates(AmzaPartitionUpdates updates) {
        this.updates = updates;
    }

    @Override
    public boolean updates(UnprefixedTxKeyValueStream txKeyValueStream) throws Exception {
        return updates.commitable(NO_OP_HIGHWATERS, txKeyValueStream);
    }
}
