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

import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class ConnectionWorker extends Thread {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final Selector selector;
    private final Queue<SocketChannel> acceptedConnections;
    private final ApplicationProtocol applicationProtocol;
    private final BufferProvider bufferProvider;
    private final MessageFramer messageFramer;
    private final ServerContext serverContext;
    private static final AtomicInteger instanceCount = new AtomicInteger();

    public ConnectionWorker(
            ApplicationProtocol applicationProtocol,
            BufferProvider bufferProvider,
            MessageFramer messageFramer,
            ServerContext serverContext) throws IOException {
        setName("TcpConnectionWorker-" + instanceCount.incrementAndGet());

        this.applicationProtocol = applicationProtocol;
        this.bufferProvider = bufferProvider;
        this.messageFramer = messageFramer;
        this.serverContext = serverContext;
        this.selector = Selector.open();
        this.acceptedConnections = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {

        LOG.info("Started Tcp connection worker");

        try {
            while (serverContext.running()) {
                try {
                    registerAccepted();
                    int ready = selector.select(500);

                    if (ready > 0) {
                        Set<SelectionKey> selected = selector.selectedKeys();
                        LOG.trace("{} connections read for processing", selected.size());

                        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                        while (keys.hasNext() && serverContext.running()) {
                            SelectionKey key = null;
                            try {
                                key = keys.next();
                                keys.remove();

                                if (key.isReadable()) {
                                    readChannel(key);
                                } else if (key.isWritable()) {
                                    writeChannel(key);
                                } else if (!key.isValid()) {
                                    closeChannel(key);
                                }
                            } catch (Exception ex) {
                                LOG.warn("Exception processing connection", ex);

                                closeChannel(key);
                            }
                        }

                    }
                } catch (IOException ioe) {
                    LOG.warn("Exception processing connections", ioe);
                }
            }

            serverContext.closeAndCatch(selector);

            //TODO dropping out of the while loop could leak in flight byte buffers
            LOG.info("Stopped Tcp connection acceptor");
        } finally {
            serverContext.closeAndCatch(selector);
        }
    }

    public void wakeup() {
        selector.wakeup();
    }

    public void addConnection(SocketChannel channel) {
        acceptedConnections.add(channel);
        selector.wakeup();
    }

    private void registerAccepted() {
        while (acceptedConnections.size() > 0) {
            SocketChannel channel = acceptedConnections.poll();
            try {
                channel.register(selector, SelectionKey.OP_READ);
                logInfoFromChannel(channel, "Registered new connection from {}");
            } catch (ClosedChannelException closed) {
                logInfoFromChannel(channel, "Newly accepted connection from {} was closed before being registered");
            }
        }
    }

    private void readChannel(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        logTraceFromChannel(socketChannel, "Reading request from connection to {}");

        InProcessServerRequest inProcess = (InProcessServerRequest) key.attachment();
        try {
            if (key.attachment() == null) {
                inProcess = new InProcessServerRequest(messageFramer, bufferProvider);
                key.attach(inProcess);
            }
            if (inProcess.readRequest(socketChannel)) {
                Message request = inProcess.getRequest();
                if (request != null) {
                    key.attach(null);

                    if (request.isLastInSequence()) {
                        Message response = applicationProtocol.handleRequest(request);
                        if (response != null) {
                            InProcessServerResponse inProcessResponse = new InProcessServerResponse(messageFramer, bufferProvider, response);
                            try {
                                key.attach(inProcessResponse);
                                key.interestOps(SelectionKey.OP_WRITE);
                            } catch (Exception ex) {
                                inProcessResponse.releaseResources();
                                throw ex;
                            }
                        }
                    } else {
                        //don't expect a resonse yet
                        applicationProtocol.handleRequest(request);
                        key.interestOps(SelectionKey.OP_READ);
                        selector.wakeup();
                    }
                } else {
                    key.interestOps(SelectionKey.OP_READ);
                    selector.wakeup();
                }
            } else {
                closeChannel(key);
            }
        } catch (Exception ioe) {
            if (inProcess != null) {
                inProcess.releaseResources();
                logWarningFromChannel(socketChannel, "Error reading connection from {}");
            }
            throw ioe;
        }
    }

    private void writeChannel(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        logTraceFromChannel(socketChannel, "Writing response to connection to {}");

        InProcessServerResponse response = (InProcessServerResponse) key.attachment();
        try {
            if (response.writeResponse(socketChannel)) {
                if (!response.isLastInSequence()) {
                    Message nextMessage = applicationProtocol.consumeSequence(response.getInteractionId());
                    if (nextMessage != null) {

                        logTraceFromChannel(socketChannel, "Writing follow on response to connection to {}");

                        response = new InProcessServerResponse(messageFramer, bufferProvider, nextMessage);
                        key.attach(response);
                        key.interestOps(SelectionKey.OP_WRITE);
                    }
                } else {
                    key.attach(null);
                    key.interestOps(SelectionKey.OP_READ);
                }
            } else {
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        } catch (IOException ioe) {
            if (response != null) {
                response.releaseResources();
                logWarningFromChannel(socketChannel, "Error writing response to connection to {}");
            }

            throw ioe;
        }
    }

    private void closeChannel(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        serverContext.closeAndCatch(channel.socket());
        serverContext.closeAndCatch(channel);
        key.attach(null);
        key.cancel();
    }

    private void logWarningFromChannel(SocketChannel channel, String message) {
        if (LOG.isWarnEnabled()) {
            try {
                LOG.warn(message, channel.getRemoteAddress());
            } catch (IOException ex) {
                LOG.error("Unable to access remote host of socket connection", ex);
            }
        }
    }

    private void logInfoFromChannel(SocketChannel channel, String message) {
        if (LOG.isInfoEnabled()) {
            try {
                LOG.info(message, channel.getRemoteAddress());
            } catch (IOException ex) {
                LOG.error("Unable to access remote host of socket connection", ex);
            }
        }
    }

    private void logTraceFromChannel(SocketChannel channel, String message) {
        if (LOG.isTraceEnabled()) {
            try {
                LOG.trace(message, channel.getRemoteAddress());
            } catch (IOException ex) {
                LOG.error("Unable to access remote host of socket connection", ex);
            }
        }
    }
}
