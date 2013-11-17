package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
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

public class SendReceiveChannel {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final RingHost host;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final int connectTimeout;
    private final int socketTimeout;
    private AtomicBoolean connected = new AtomicBoolean();
    private SocketChannel socketChannel;
    private ReadableByteChannel readChannel;

    public SendReceiveChannel(RingHost host, int connectTimeout, int socketTimeout, int readBufferSize, int writeBufferSize) {
        this.host = host;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
    }

    RingHost getRingHost() {
        return host;
    }

    public void connect() throws IOException {
        if (connected.compareAndSet(false, true)) {
            try {
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
                socketChannel.socket().setTcpNoDelay(true);
                socketChannel.socket().setReuseAddress(true);
                socketChannel.socket().connect(new InetSocketAddress(host.getHost(), host.getPort()), connectTimeout);
                readChannel = Channels.newChannel(socketChannel.socket().getInputStream());

                LOG.info("Created socket channel connected to {}.", new Object[]{socketTimeout, readBufferSize, writeBufferSize, host});
            } catch (Throwable t) {
                connected.set(false);
                throw t;
            }
        }
    }

    public void reconnect() throws IOException {
        disconect();
        connect();
    }

    private void closeAndCatch(Closeable channel) {
        try {
            channel.close();
        } catch (IOException ex) {
            LOG.warn("Error closing socket channel", ex);
        }
    }

    public void disconect() {
        if (connected.compareAndSet(true, false)) {
            closeAndCatch(socketChannel);
            closeAndCatch(socketChannel.socket());
            closeAndCatch(readChannel);

            socketChannel = null;
            readChannel = null;
        }
    }

    public void send(ByteBuffer buffer) throws ClosedChannelException, IOException {
        if (!connected.get()) {
            throw new ClosedChannelException();
        }

        socketChannel.write(buffer);

    }

    public int receive(ByteBuffer buffer) throws ClosedChannelException, IOException {
        if (!connected.get()) {
            throw new ClosedChannelException();
        }

        return readChannel.read(buffer);
    }
}
