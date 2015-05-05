package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.IOException;

/**
 *
 */
public interface ResponseWriter {

    void writeMessage(Message message) throws IOException;
}
