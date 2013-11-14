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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Creates and pools TcpClients.
 */
public class TcpClientProvider {

    private final ConcurrentMap<RingHost, ChannelPool> channelPools;
    private final int connectionsPerHost;
    private final int connectTimeout;
    private final int socketTimeout;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final BufferProvider bufferProvider;
    private final MessageFramer messageFramer;

    public TcpClientProvider(int connectionsPerHost, int connectTimeout, int socketTimeout, int readBufferSize, int writeBufferSize,
        BufferProvider bufferProvider, MessageFramer messageFramer) {
        this.connectionsPerHost = connectionsPerHost;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.channelPools = new ConcurrentHashMap<>();
        this.bufferProvider = bufferProvider;
        this.messageFramer = messageFramer;
    }

    /**
     * Returns a connected tcp client for the supplied ring host. TcpClient's are meant for thread per connection style communication. This means only let one
     * thread use a given TcpClient at a time.
     *
     * @param ringHost the host to connect to
     * @return a connected tcp client
     * @throws IOException
     */
    public TcpClient getClientForHost(RingHost ringHost) throws IOException, InterruptedException {
        ChannelPool pool = channelPools.get(ringHost);
        if (pool == null) {
            ChannelPool newPool = new ChannelPool(ringHost, connectionsPerHost);
            ChannelPool existing = channelPools.putIfAbsent(ringHost, newPool);
            pool = existing == null ? newPool : existing;
        }

        return new TcpClient(pool.acquireChannel(), bufferProvider, messageFramer);
    }

    public void returnClient(TcpClient client) {
        SendReceiveChannel channel = client.getChannel();
        ChannelPool pool = channelPools.get(channel.getRingHost());
        if (pool == null) {
            throw new IllegalStateException("No pool exists for host: " + channel.getRingHost());
        }

        pool.releaseChannel(channel);
    }

    private class ChannelPool {

        private final BlockingQueue<SendReceiveChannel> channels;

        private ChannelPool(RingHost ringHost, int connectionsPerHost) {
            channels = new LinkedBlockingQueue<>(connectionsPerHost);
            for (int i = 0; i < connectionsPerHost; i++) {
                channels.add(new SendReceiveChannel(
                    ringHost, connectTimeout, socketTimeout, readBufferSize, writeBufferSize));
            }
        }

        private SendReceiveChannel acquireChannel() throws IOException, InterruptedException {
            SendReceiveChannel channel = channels.take();
            channel.connect();
            return channel;
        }

        private boolean releaseChannel(SendReceiveChannel channel) {
            return channels.offer(channel);
        }
    }
}
