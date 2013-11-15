package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ClientChannelProvider {

    private ConcurrentMap<RingHost, ClientChannel> channelPool;
    private final int connectTimeout;
    private final int socketTimeout;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final MessageFramer framer;
    private final BufferProvider bufferProvider;
    private final Object createLock = new Object();

    public ClientChannelProvider(int connectTimeout, int socketTimeout, int readBufferSize, int writeBufferSize, MessageFramer framer,
            BufferProvider bufferProvider) {
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.framer = framer;
        this.bufferProvider = bufferProvider;
        this.channelPool = new ConcurrentHashMap<>();
    }

    public ClientChannel getChannelForHost(RingHost ringHost) throws IOException {
        ClientChannel channel = channelPool.get(ringHost);
        if (channel == null) {
            synchronized (createLock) {
                channel = channelPool.get(ringHost);
                if (channel == null) {
                    channel = new ClientChannel(ringHost, socketTimeout, readBufferSize, writeBufferSize, framer, bufferProvider);
                    channelPool.put(ringHost, channel);
                }
            }
        }

        channel.connect(connectTimeout);

        //todo test and repair channel here
        return channel;

    }

    public void closeAll() {
        synchronized (createLock) {
            for (ClientChannel channel : channelPool.values()) {
                channel.disconect();
            }
            channelPool.clear();
        }
    }
}
