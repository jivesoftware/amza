package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
import java.io.IOException;

/**
 *
 */
public class TcpServerInitializer {

    public TcpServer initialize(RingHost localhost, int numWorkers, BufferProvider bufferProvider,
        MessageFramer messageFramer, ApplicationProtocol requestHandler) throws IOException {
        ServerContext serverContext = new ServerContext();
        ConnectionWorker[] connectionWorkers = new ConnectionWorker[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            connectionWorkers[i] = new ConnectionWorker(requestHandler, bufferProvider, messageFramer, serverContext);
        }

        ConnectionAcceptor connectionAcceptor = new ConnectionAcceptor(localhost, connectionWorkers, serverContext);

        return new TcpServer(connectionAcceptor, connectionWorkers, serverContext);
    }
}
