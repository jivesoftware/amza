package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer.Frame;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Channel designed to write small messages in a blocking manner, and to stream back sequences of messages in a blocking manner.
 */
public class ClientChannel {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingHost host;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final int socketTimeout;
    private final MessageFramer framer;
    private final BufferProvider bufferProvider;
    private AtomicBoolean connected = new AtomicBoolean();
    private SocketChannel socketChannel;
    private ReadableByteChannel readChannel;
    private final Object connectLock = new Object();

    public ClientChannel(RingHost host, int socketTimeout, int readBufferSize, int writeBufferSize, MessageFramer framer,
        BufferProvider bufferProvider) {
        this.host = host;
        this.socketTimeout = socketTimeout;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.framer = framer;
        this.bufferProvider = bufferProvider;
    }

    public void connect(int connectTimeout) throws IOException {
        if (!connected.get()) {
            synchronized (connectLock) {
                if (!connected.get()) {
                    socketChannel = socketChannel.open();
                    if (readBufferSize > 0) {
                        socketChannel.socket().setReceiveBufferSize(readBufferSize);
                    }
                    if (writeBufferSize > 0) {
                        socketChannel.socket().setSendBufferSize(writeBufferSize);
                    }
                    socketChannel.configureBlocking(true);
                    socketChannel.socket().setSoTimeout(socketTimeout);
                    socketChannel.socket().setKeepAlive(true);
                    socketChannel.socket().connect(new InetSocketAddress(host.getHost(), host.getPort()), connectTimeout);
                    readChannel = Channels.newChannel(socketChannel.socket().getInputStream());

                    LOG.info("Created socket channel connected to {}.", new Object[]{socketTimeout, readBufferSize, writeBufferSize, host});
                    connected.set(true);
                }
            }
        }
    }

    private void closeAndCatch(Closeable channel) {
        try {
            channel.close();
        } catch (IOException ex) {
            LOG.warn("Error closing socket channel", ex);
        }
    }

    public void disconect() {
        if (connected.get()) {
            synchronized (connectLock) {
                if (connected.get()) {
                    closeAndCatch(socketChannel);
                    closeAndCatch(socketChannel.socket());
                    closeAndCatch(readChannel);

                    socketChannel = null;
                    readChannel = null;
                    connected.set(false);
                }
            }
        }
    }

    public void send(byte[] message, boolean lastInSequence) throws ClosedChannelException, IOException {
        if (!connected.get()) {
            throw new ClosedChannelException();
        }

        ByteBuffer writeBuffer = bufferProvider.acquire();
        try {
            framer.buildFrame(message, lastInSequence, writeBuffer);
            socketChannel.write(writeBuffer);
        } finally {
            bufferProvider.release(writeBuffer);
        }
    }

    public void receive(CallbackStream<byte[]> messageStream, ByteBuffer receiveBuffer) throws ClosedChannelException, IOException {
        if (!connected.get()) {
            throw new ClosedChannelException();
        }

        boolean eos = false;
        while (!eos) {
            readChannel.read(receiveBuffer);

            Frame frame = framer.readFrame(receiveBuffer);
            if (frame != null) {
                try {
                    messageStream.callback(frame.getMessage());
                    if (frame.isLastInSequence()) {
                        messageStream.callback(null);
                    }
                } catch (Exception ex) {
                    LOG.error("Error invoking callback when reading stream from host {}", new Object[]{host}, ex);
                }
            }
        }
    }
}
