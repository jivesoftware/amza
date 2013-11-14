package com.jivesoftware.os.amza.transport.tcp.replication.client;

import com.jivesoftware.os.amza.transport.tcp.replication.shared.ChangeSet;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.FstMarshaller;
import com.jivesoftware.os.amza.shared.ChangeSetSender;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import java.util.NavigableMap;

/**
 *
 */
public class TCPChangeSetSender implements ChangeSetSender {
    private final ClientChannelProvider clientChannelProvider;
    private final FstMarshaller fstMarshaller;

    public TCPChangeSetSender(ClientChannelProvider clientChannelProvider, FstMarshaller fstMarshaller) {
        this.clientChannelProvider = clientChannelProvider;
        this.fstMarshaller = fstMarshaller;
    }
    

    @Override
    public <K, V> void sendChangeSet(RingHost ringHost, TableName<K, V> mapName, NavigableMap<K, TimestampedValue<V>> changes) throws Exception {
        ClientChannel channel = clientChannelProvider.getChannelForHost(ringHost);
        ChangeSet changeSet= new ChangeSet(mapName, changes);
        byte[] serialized = fstMarshaller.serialize(changeSet);
        channel.send(serialized, true);
    }

}
