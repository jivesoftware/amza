package com.jivesoftware.os.amza.transport.tcp.replication;

import com.jivesoftware.os.amza.shared.ChangeSetTaker;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClient;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;

/**
 *
 */
public class TcpChangeSetTaker implements ChangeSetTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TcpClientProvider clientProvider;
    private final IndexReplicationProtocol indexReplicationProtocol;

    public TcpChangeSetTaker(TcpClientProvider clientProvider, IndexReplicationProtocol indexReplicationProtocol) {
        this.clientProvider = clientProvider;
        this.indexReplicationProtocol = indexReplicationProtocol;
    }

    @Override
    public <K, V> void take(RingHost ringHost, TableName<K, V> tableName,
        long transationId, final TransactionSetStream transactionSetStream) throws Exception {
        TcpClient client = clientProvider.getClientForHost(ringHost);
        try {
            Message message = new Message(indexReplicationProtocol.nextInteractionId(),
                indexReplicationProtocol.OPCODE_REQUEST_CHANGESET, true);
            client.sendMessage(message);

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

            Message entry = null;
            boolean streamingResults = true;

            while ((entry = client.receiveMessage()) != null) {

                //if we aren't dispatching results anymore, we still need to loop over the input to drain the socket
                if (streamingResults) {
                    try {
                        TransactionSet payload = entry.getPayload();
                        TransactionSet returned = messageStream.callback(payload);
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
            clientProvider.returnClient(client);
        }
    }
}
