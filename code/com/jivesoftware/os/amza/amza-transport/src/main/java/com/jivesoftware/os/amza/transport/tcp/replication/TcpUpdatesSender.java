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

import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.RowScanable;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.UpdatesSender;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.RowUpdatesPayload;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClient;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class TcpUpdatesSender implements UpdatesSender {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TcpClientProvider tcpClientProvider;
    private final IndexReplicationProtocol indexReplicationProtocol;

    public TcpUpdatesSender(TcpClientProvider tcpClientProvider, IndexReplicationProtocol indexReplicationProtocol) {
        this.tcpClientProvider = tcpClientProvider;
        this.indexReplicationProtocol = indexReplicationProtocol;
    }

    @Override
    public void sendUpdates(final RingHost ringHost, final TableName tableName, RowScanable changes) throws Exception {
        final int batchSize = 10; // TODO expose to config;
        final List<RowIndexKey> keys = new ArrayList<>();
        final List<RowIndexValue> values = new ArrayList<>();
        final MutableLong highestId = new MutableLong(-1);
        changes.rowScan(new RowScan<Exception>() {
            @Override
            public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws Exception {
                keys.add(key);
                // We make this copy because we don't know how the value is being stored. By calling value.getValue()
                // we ensure that the value from the tableIndex is real vs a pointer.
                RowIndexValue copy = new RowIndexValue(value.getValue(), value.getTimestamp(), value.getTombstoned());
                values.add(copy);
                if (highestId.longValue() < orderId) {
                    highestId.setValue(orderId);
                }
                if (keys.size() > batchSize) {
                    sendBatch(ringHost, new RowUpdatesPayload(tableName, highestId.longValue(), keys, values));
                    keys.clear();
                    values.clear();
                }
                return true;
            }
        });

        if (!keys.isEmpty()) {
            sendBatch(ringHost, new RowUpdatesPayload(tableName, highestId.longValue(), keys, values));
        }
    }

    private void sendBatch(RingHost ringHost, RowUpdatesPayload payload) throws Exception {

        TcpClient client = tcpClientProvider.getClientForHost(ringHost);
        try {
            LOG.debug("Pushing " + payload.size() + " changes to " + ringHost);
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
