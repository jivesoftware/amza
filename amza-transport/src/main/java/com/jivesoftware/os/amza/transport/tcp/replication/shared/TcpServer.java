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

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;

/**
 *
 */
public class TcpServer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final ConnectionWorker[] connectionWorkers;
    private final ConnectionAcceptor connectionAcceptor;
    private final ServerContext serverContext;

    public TcpServer(ConnectionAcceptor connectionAcceptor, ConnectionWorker[] connectionWorkers,
        ServerContext serverContext) throws IOException {
        this.connectionWorkers = connectionWorkers;
        this.connectionAcceptor = connectionAcceptor;
        this.serverContext = serverContext;
    }

    public void start() throws InterruptedException {
        LOG.info("Starting TcpServer...");
        if (serverContext.start()) {
            for (ConnectionWorker worker : connectionWorkers) {
                worker.start();
            }

            connectionAcceptor.start();
        }
        LOG.inc("TcpServer started");

    }

    public void stop() throws InterruptedException {
        LOG.info("Stopping TcpServer...");
        if (serverContext.stop()) {
            connectionAcceptor.wakeup();

            connectionAcceptor.join();

            for (ConnectionWorker worker : connectionWorkers) {
                worker.wakeup();
                worker.join();
            }
        }
        LOG.info("TcpServer stopped");
    }
}
