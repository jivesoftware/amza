package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.Serializable;

/**
 *
 */
public interface ApplicationProtocol {

    Message handleRequest(Message request);

    Message consumeSequence(long interactionId);

    Class<? extends Serializable> getOperationPayloadClass(int opCode);

    long nextInteractionId();
}
