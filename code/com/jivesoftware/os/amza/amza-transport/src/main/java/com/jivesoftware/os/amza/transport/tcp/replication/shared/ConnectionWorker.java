package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ConnectionWorker extends Thread {

    private final Selector selector;
    private final Queue<SocketChannel> acceptedConnections;
    private final ServerRequestHandler requestHandler;
    private final BufferProvider bufferProvider;
    private final MessageFramer messageFramer;
    private final ServerContext serverContext;
    private static final AtomicInteger instanceCount = new AtomicInteger();

    public ConnectionWorker(
        ServerRequestHandler requestHandler,
        BufferProvider bufferProvider,
        MessageFramer messageFramer,
        ServerContext serverContext) throws IOException {
        setName("TcpConnectionWorker-" + instanceCount.incrementAndGet());

        this.requestHandler = requestHandler;
        this.bufferProvider = bufferProvider;
        this.messageFramer = messageFramer;
        this.serverContext = serverContext;
        this.selector = Selector.open();
        this.acceptedConnections = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {

        try {
            while (serverContext.running()) {
                try {
                    registerAccepted();
                    int ready = selector.select(500);

                    if (ready > 0) {
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
                                closeChannel(key);
                            }
                        }

                    }
                } catch (IOException ioe) {
                }
            }
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
            } catch (ClosedChannelException closed) {
                //they bailed before we did anything with the connection
            }
        }
    }

    private void readChannel(SelectionKey key) throws Exception {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        InProcessRequest inProcess = (InProcessRequest) key.attachment();
        if (key.attachment() == null) {
            key.attach(new InProcessRequest());
        }
        if (inProcess.readRequest(socketChannel)) {
            FrameableMessage request = inProcess.getRequest();
            if (request != null) {
                key.attach(null);

                if (request.isLastInSequence()) {
                    FrameableMessage response = requestHandler.handleRequest(request);
                    if (response != null) {
                        key.attach(new InProcessResponse(response));
                        key.interestOps(SelectionKey.OP_WRITE);
                    }
                } else {
                    //don't expect a resonse yet
                    requestHandler.handleRequest(request);
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
    }

    private void writeChannel(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        InProcessResponse response = (InProcessResponse) key.attachment();

        if (response.writeResponse(socketChannel)) {
            if (!response.isLastInSequence()) {
                FrameableMessage nextMessage = requestHandler.consumeSequence(response.getInteractionId());
                if (nextMessage != null) {
                    key.attach(new InProcessResponse(nextMessage));
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
    }

    private void closeChannel(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        serverContext.closeAndCatch(channel.socket());
        serverContext.closeAndCatch(channel);
        key.attach(null);
        key.cancel();
    }

    private class InProcessRequest {

        private final ByteBuffer readBuffer;
        private final AtomicReference<FrameableMessage> message;

        private InProcessRequest() {
            readBuffer = bufferProvider.acquire();
            message = new AtomicReference<>();
        }

        private boolean readRequest(SocketChannel channel) throws Exception {
            try {
                int read = channel.read(readBuffer);

                if (read > 0) {
                    FrameableMessage request = messageFramer.fromFrame(readBuffer, FrameableMessage.class); //???
                    if (request != null) {
                        message.set(request);
                        bufferProvider.release(readBuffer);
                    }

                    return true;
                } else {
                    bufferProvider.release(readBuffer);

                    return false;
                }
            } catch (Exception ex) {
                bufferProvider.release(readBuffer);
                throw ex;
            }
        }

        public FrameableMessage getRequest() {
            return message.get();
        }
    }

    private class InProcessResponse {

        private final ByteBuffer writeBuffer;
        private final long interactionId;
        private final boolean lastInSequence;

        private InProcessResponse(FrameableMessage response) throws IOException {
            writeBuffer = bufferProvider.acquire();
            messageFramer.toFrame(response, writeBuffer);

            this.interactionId = response.getInteractionId();
            this.lastInSequence = response.isLastInSequence();
        }

        public long getInteractionId() {
            return interactionId;
        }

        public boolean isLastInSequence() {
            return lastInSequence;
        }

        private boolean writeResponse(SocketChannel channel) throws IOException {
            try {
                channel.write(writeBuffer);

                if (writeBuffer.remaining() == 0) {
                    bufferProvider.release(writeBuffer);
                    return true;
                } else {
                    return false;
                }
            } catch (Exception ex) {
                bufferProvider.release(writeBuffer);
                throw ex;
            }
        }
    }
}
