package com.jivesoftware.os.amza.transport.netty.server;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class AmzaReplicationNettyServer {
    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final int port;
    private final AmzaInstance amzaInstance;

    public AmzaReplicationNettyServer(int port, AmzaInstance amzaInstance) {
        this.port = port;
        this.amzaInstance = amzaInstance;
    }


    public void run() {
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new AmzaReplicationServerHandler(amzaInstance));
            }
        });

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(port));
    }
}
