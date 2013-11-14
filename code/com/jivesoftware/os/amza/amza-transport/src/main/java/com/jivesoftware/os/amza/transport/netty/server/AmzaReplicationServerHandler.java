package com.jivesoftware.os.amza.transport.netty.server;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class AmzaReplicationServerHandler extends SimpleChannelUpstreamHandler {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaInstance amzaInstance;

    public AmzaReplicationServerHandler(AmzaInstance amzaInstance) {
        this.amzaInstance = amzaInstance;
    }

    private final AtomicLong transferredBytes = new AtomicLong();

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        ChannelBuffer channelBuffer = (ChannelBuffer) e.getMessage();

        // TODO
//        // Send back the received message to the remote peer.
//        transferredBytes.addAndGet(((ChannelBuffer) e.getMessage()).readableBytes());
//        e.getChannel().write(e.getMessage());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        // Close the connection when an exception is raised.
        LOG.warn("Unexpected exception from downstream.", e.getCause());
        e.getChannel().close();
    }
}