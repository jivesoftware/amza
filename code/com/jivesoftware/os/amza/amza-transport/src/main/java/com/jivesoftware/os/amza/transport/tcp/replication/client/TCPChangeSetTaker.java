package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.ChangeSetRequest;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.ChangeSetResponse;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.BufferProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFramer;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.SendReceiveChannel;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.SendReceiveChannelProvider;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.nio.ByteBuffer;

/**
 *
 */
public class TCPChangeSetTaker implements ChangeSetTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final SendReceiveChannelProvider clientChannelProvider;
    private final MessageFramer messageFramer;
    private final BufferProvider bufferProvider;

    public TCPChangeSetTaker(SendReceiveChannelProvider clientChannelProvider, MessageFramer messageFramer, BufferProvider bufferProvider) {
        this.clientChannelProvider = clientChannelProvider;
        this.messageFramer = messageFramer;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public synchronized <K, V> void take(RingHost ringHost, TableName<K, V> tableName, long transationId,
        final TransactionSetStream transactionSetStream) throws Exception {
        SendReceiveChannel channel = clientChannelProvider.getChannelForHost(ringHost);

        ByteBuffer sendBuff = bufferProvider.acquire();
        try {
            messageFramer.toFrame(new ChangeSetRequest(), sendBuff);
            channel.send(sendBuff);

            sendBuff.clear();

            CallbackStream<TransactionSet> messageStream = new CallbackStream<TransactionSet>() {
                @Override
                public TransactionSet callback(TransactionSet transactionSet) throws Exception {
                    if (transactionSet != null) {
                        return transactionSetStream.stream(transactionSet) ? transactionSet : null;
                    } else {
                        return null;
                    }
                }
            };

            ChangeSetResponse entry = null;
            boolean streamingResults = true;

            while ((entry = readChangeSetResponse(channel, sendBuff)) != null) {

                //if we aren't dispatching results anymore, we still need to loop over the input to drain the socket
                if (streamingResults) {
                    try {
                        TransactionSet returned = messageStream.callback(entry.getTransactionSet());
                        if (returned == null) {
                            streamingResults = false;
                        }
                    } catch (Exception ex) {
                        LOG.error("Error streaming in transacion set {}", new Object[]{entry}, ex);
                    }
                }

                if (entry.isLastInSequence()) {
                    break;
                }
            }

        } finally {
            bufferProvider.release(sendBuff);
        }
    }

    private ChangeSetResponse readChangeSetResponse(SendReceiveChannel sendReceiveChannel, ByteBuffer readBuffer) throws Exception {
        ChangeSetResponse response = null;
        int read = 0;
        int lastRead = 0;

        while (response == null && lastRead >= 0) {
            lastRead = sendReceiveChannel.receive(readBuffer);
            read += lastRead;
            response = messageFramer.<ChangeSetResponse>fromFrame(readBuffer, read, ChangeSetResponse.class);
        }

        return response;
    }
}
