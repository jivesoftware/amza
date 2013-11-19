package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 *
 */
public class ConnectionAcceptor extends Thread {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final Selector selector;
    private final RingHost localHost;
    private final ConnectionWorker[] workers;
    private final ServerContext serverContext;

    public ConnectionAcceptor(RingHost localHost, ConnectionWorker[] connectionWorkers, ServerContext serverContext)
        throws IOException {
        setName("TcpConnectionAcceptor");

        this.localHost = localHost;
        this.workers = connectionWorkers;
        this.serverContext = serverContext;
        this.selector = Selector.open();
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

            LOG.info("Started Tcp connection acceptor bound to {}", localHost);

            int workerIdx = 0;

            while (serverContext.running()) {
                int ready = selector.select(500);
                if (ready > 0) {
                    Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                    while (keys.hasNext() && serverContext.running()) {
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

            serverContext.closeAndCatch(channel);
            serverContext.closeAndCatch(selector);

            LOG.info("Stopped Tcp connection acceptor bound to {}", localHost);

        } catch (Exception ex) {
            serverContext.closeAndCatch(channel);
            serverContext.closeAndCatch(selector);
        }
    }

    public void wakeup() {
        selector.wakeup();
    }

    private void acceptConnection(SelectionKey key, ConnectionWorker worker) throws IOException {
        SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();

        //todo channel options ??

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        worker.addConnection(channel);

        LOG.info("Accepted connection from {}", channel.getRemoteAddress());
    }
}
