/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
