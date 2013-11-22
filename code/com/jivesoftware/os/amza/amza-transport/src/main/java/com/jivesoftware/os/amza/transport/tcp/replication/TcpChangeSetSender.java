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
            Message request = new Message(indexReplicationProtocol.nextInteractionId(),
                    indexReplicationProtocol.OPCODE_PUSH_CHANGESET, true, payload);
            client.sendMessage(request);

            Message response = client.receiveMessage();
            if (response == null) {
                throw new Exception("No response received for message:" + request);
            }

            int opCode = response.getOpCode();
            if (opCode == indexReplicationProtocol.OPCODE_ERROR) {
                String errorMsg = response.getPayload();
                throw new Exception(errorMsg);
            } else if (opCode != indexReplicationProtocol.OPCODE_OK) {
                throw new IllegalArgumentException("Unexpected opcode in response to change set send:" + opCode);
            }

        } finally {
            tcpClientProvider.returnClient(client);
        }

    }
}
