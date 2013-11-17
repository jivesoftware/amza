package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;

/**
 *
 */
public interface ServerRequestHandler {

    FrameableMessage handleRequest(FrameableMessage request);
}
