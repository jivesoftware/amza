package com.jivesoftware.os.amza.transport.tcp.replication.shared;

/**
 *
 */
public interface ServerRequestHandler {

    MessageFrame handleRequest(MessageFrame request);

    MessageFrame consumeSequence(long interactionId);
}
