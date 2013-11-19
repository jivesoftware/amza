package com.jivesoftware.os.amza.transport.tcp.replication;

import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.SendChangeSetPayload;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClient;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import java.util.NavigableMap;

/**
 *
 */
public class TcpChangeSetSender implements ChangeSetSender {

    private final TcpClientProvider tcpClientProvider;
    private final IndexReplicationProtocol indexReplicationProtocol;

    public TcpChangeSetSender(TcpClientProvider tcpClientProvider, IndexReplicationProtocol indexReplicationProtocol) {
        this.tcpClientProvider = tcpClientProvider;
        this.indexReplicationProtocol = indexReplicationProtocol;
    }

    @Override
    public <K, V> void sendChangeSet(RingHost ringHost, TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        TcpClient client = tcpClientProvider.getClientForHost(ringHost);
        try {
            SendChangeSetPayload payload = new SendChangeSetPayload(mapName, changes);
            client.sendMessage(new Message(indexReplicationProtocol.nextInteractionId(),
                indexReplicationProtocol.OPCODE_PUSH_CHANGESET, true, payload));
        } finally {
            tcpClientProvider.returnClient(client);
        }

    }
}
