package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.FrameableMessage;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class TcpServer {

    //TODO logging, exception handling, break up into smaller classes
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingHost localHost;
    private final ServerRequestHandler requestHandler;
    private final BufferProvider bufferProvider;
    private final MessageFramer messageFramer;
    private final Worker[] workers;
    private final Boss boss;
    private final AtomicBoolean running = new AtomicBoolean();
    private final CountDownLatch startUpLatch;

    public TcpServer(RingHost localHost, int numWorkerThreads, ServerRequestHandler requestHandler,
        BufferProvider bufferProvider, MessageFramer messageFramer) throws IOException {
        this.localHost = localHost;
        this.requestHandler = requestHandler;
        this.bufferProvider = bufferProvider;
        this.messageFramer = messageFramer;
        this.boss = new Boss();
        this.workers = new Worker[numWorkerThreads];

        for (int i = 0; i < numWorkerThreads; i++) {
            workers[i] = new Worker(i + 1);
        }

        startUpLatch = new CountDownLatch(numWorkerThreads);
    }

    public void start() throws InterruptedException {
        if (running.compareAndSet(false, true)) {
            for (Worker worker : workers) {
                worker.start();
            }

            startUpLatch.await();

            boss.start();
        }
    }

    public void stop() throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            boss.halt();

            boss.join();

            for (Worker worker : workers) {
                worker.halt();
            }

            for (Worker worker : workers) {
                worker.join();
            }
        }
    }

    private void closeAndCatch(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ex) {
            }
        }
    }

    private class Boss extends Thread {

        private final Selector selector;

        public Boss() throws IOException {
            setName("TcpServer-ConnectionAcceptor");
            selector = Selector.open();
        }

        @Override
        public void run() {
            ServerSocketChannel channel = null;

            try {
                channel = ServerSocketChannel.open();

                //TODO channel options?

                channel.configureBlocking(false);
                channel.socket().bind(new InetSocketAddress(localHost.getHost(), localHost.getPort()));
                channel.register(selector, SelectionKey.OP_ACCEPT);

                int workerIdx = 0;

                while (running.get()) {
                    int ready = selector.select(500);
                    if (ready > 0) {
                        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                        while (keys.hasNext() && running.get()) {
                            try {
                                SelectionKey key = keys.next();
                                keys.remove();

                                if (key.isAcceptable()) {
                                    acceptConnection(key, workers[workerIdx]);
                                }

                                workerIdx = (workerIdx + 1) % workers.length;
                            } catch (Exception e) {
                            }
                        }
                    }
                }

                closeAndCatch(channel);
                closeAndCatch(selector);

            } catch (Exception ex) {
                closeAndCatch(channel);
                closeAndCatch(selector);
            }
        }

        private void halt() {
            selector.wakeup();
        }

        private void acceptConnection(SelectionKey key, Worker worker) throws IOException {
            SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();

            //todo channel options ??

            channel.configureBlocking(false);
            channel.socket().setTcpNoDelay(true);
            worker.addConnection(channel);
        }
    }

    private class Worker extends Thread {

        private final Selector selector;
        private final Queue<SocketChannel> accepted;

        public Worker(int idx) throws IOException {
            setName("TcpServer-ConnectionProcessor-" + idx);
            selector = Selector.open();
            accepted = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void run() {
            startUpLatch.countDown();

            try {
                while (running.get()) {
                    try {
                        registerAccepted();
                        int ready = selector.select(500);

                        if (ready > 0) {
                            Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                            while (keys.hasNext()) {
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
                closeAndCatch(selector);
            }
        }

        private void halt() {
            selector.wakeup();
        }

        private void addConnection(SocketChannel channel) {
            accepted.add(channel);
            selector.wakeup();
        }

        private void registerAccepted() {
            while (accepted.size() > 0) {
                SocketChannel channel = accepted.poll();
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
                    FrameableMessage response = requestHandler.handleRequest(request);
                    if (response != null) {
                        key.attach(new InProcessResponse(response));
                        key.interestOps(SelectionKey.OP_WRITE);
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
                key.attach(null);
                key.interestOps(SelectionKey.OP_READ);
            } else {
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        }

        private void closeChannel(SelectionKey key) {
            SocketChannel channel = (SocketChannel) key.channel();
            closeAndCatch(channel.socket());
            closeAndCatch(channel);
            key.attach(null);
            key.cancel();
        }
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

        private InProcessResponse(FrameableMessage response) throws IOException {
            writeBuffer = bufferProvider.acquire();
            messageFramer.toFrame(response, writeBuffer);
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
