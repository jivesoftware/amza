package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.SendChangeSet;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.IdProvider;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.MessageFrame;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.OpCodes;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClient;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import java.util.NavigableMap;

/**
 *
 */
public class TcpChangeSetSender implements ChangeSetSender {

    private final TcpClientProvider tcpClientProvider;
    private final IdProvider idProvider;

    public TcpChangeSetSender(TcpClientProvider tcpClientProvider, IdProvider idProvider) {
        this.tcpClientProvider = tcpClientProvider;
        this.idProvider = idProvider;
    }

    @Override
    public <K, V> void sendChangeSet(RingHost ringHost, TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        TcpClient client = tcpClientProvider.getClientForHost(ringHost);
        try {
            SendChangeSet payload = new SendChangeSet(mapName, changes);
            client.sendMessage(new MessageFrame(idProvider.nextId(), OpCodes.OPCODE_PUSH_CHANGESET, true, payload));
        } finally {
            tcpClientProvider.returnClient(client);
        }

    }
}
