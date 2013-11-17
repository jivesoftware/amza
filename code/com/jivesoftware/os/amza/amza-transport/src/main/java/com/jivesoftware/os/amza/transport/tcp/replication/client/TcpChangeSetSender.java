package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.transport.tcp.replication.messages.SendChangeSet;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClient;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import java.util.NavigableMap;

/**
 *
 */
public class TcpChangeSetSender implements ChangeSetSender {

    private final TcpClientProvider tcpClientProvider;

    public TcpChangeSetSender(TcpClientProvider tcpClientProvider) {
        this.tcpClientProvider = tcpClientProvider;
    }

    @Override
    public <K, V> void sendChangeSet(RingHost ringHost, TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        TcpClient client = tcpClientProvider.getClientForHost(ringHost);
        try {
            client.sendMessage(new SendChangeSet(mapName, changes));
        } finally {
            tcpClientProvider.returnClient(client);
        }

    }
}
