package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.io.IOException;

/**
 *
 */
public interface Response {

    Message getMessage();

    boolean isBlocking();

    boolean hasMessage();

    void writeTo(ResponseWriter responseWriter) throws IOException;
}
