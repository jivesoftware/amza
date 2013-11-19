package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.ChangeSetResponse;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.SendChangeSet;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class OpCodes {

    public static final int OPCODE_PUSH_CHANGESET = 3;
    public static final int OPCODE_REQUEST_CHANGESET = 5;
    public static final int OPCODE_RESPOND_CHANGESET = 7;
    private final Map<Integer, Class<? extends Serializable>> payloadRegistry;

    public OpCodes() {
        Map<Integer, Class<? extends Serializable>> map = new HashMap<>();
        map.put(OPCODE_PUSH_CHANGESET, SendChangeSet.class);
        map.put(OPCODE_RESPOND_CHANGESET, ChangeSetResponse.class);
        payloadRegistry = Collections.unmodifiableMap(map);
    }

    public Map<Integer, Class<? extends Serializable>> getPayloadRegistry() {
        return payloadRegistry;
    }
}
