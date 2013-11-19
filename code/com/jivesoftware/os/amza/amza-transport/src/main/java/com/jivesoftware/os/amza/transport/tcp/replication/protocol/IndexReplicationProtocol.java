package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.transport.tcp.replication.shared.ApplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class IndexReplicationProtocol implements ApplicationProtocol {

    public final int OPCODE_PUSH_CHANGESET = 3;
    public final int OPCODE_REQUEST_CHANGESET = 5;
    public final int OPCODE_RESPOND_CHANGESET = 7;
    private final Map<Integer, Class<? extends Serializable>> payloadRegistry;
    private final IdProvider idProvider;

    public IndexReplicationProtocol(IdProvider idProvider) {
        this.idProvider = idProvider;

        Map<Integer, Class<? extends Serializable>> map = new HashMap<>();
        map.put(OPCODE_PUSH_CHANGESET, SendChangeSetPayload.class);
        map.put(OPCODE_RESPOND_CHANGESET, ChangeSetResponsePayload.class);
        payloadRegistry = Collections.unmodifiableMap(map);
    }

    @Override
    public Message handleRequest(Message request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Message consumeSequence(long interactionId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Class<? extends Serializable> getOperationPayloadClass(int opCode) {
        return payloadRegistry.get(opCode);
    }

    @Override
    public long nextInteractionId() {
        return idProvider.nextId();
    }
}
