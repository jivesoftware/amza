package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Creates and pools SendReceiveChannels.
 */
public class SendReceiveChannelProvider {

    //TODO implement pool as a multimap and introduce a connections per host param to bound it. This way we can round robin between
    //a set of connections per host
    private ConcurrentMap<RingHost, SendReceiveChannel> channelPool;
    private final int connectTimeout;
    private final int socketTimeout;
    private final int readBufferSize;
    private final int writeBufferSize;

    public SendReceiveChannelProvider(int connectTimeout, int socketTimeout, int readBufferSize, int writeBufferSize) {
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.channelPool = new ConcurrentHashMap<>();
    }

    public SendReceiveChannel getChannelForHost(RingHost ringHost) throws IOException {
        SendReceiveChannel channel = channelPool.get(ringHost);
        if (channel == null) {
            SendReceiveChannel newChannel = new SendReceiveChannel(ringHost, socketTimeout, readBufferSize, writeBufferSize);
            SendReceiveChannel existing = channelPool.putIfAbsent(ringHost, newChannel);
            channel = existing == null ? newChannel : existing;
        }

        channel.connect(connectTimeout);

        //todo test and repair channel here

        return channel;
    }

    public void closeAll() {
        for (SendReceiveChannel channel : channelPool.values()) {
            channel.disconect();
        }
        channelPool.clear();
    }
}
