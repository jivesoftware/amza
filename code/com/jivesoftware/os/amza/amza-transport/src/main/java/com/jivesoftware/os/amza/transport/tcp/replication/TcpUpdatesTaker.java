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

import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.UpdatesTaker;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.IndexReplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.RowUpdatesPayload;
import com.jivesoftware.os.amza.transport.tcp.replication.protocol.TakeRowUpdatesRequestPayload;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClient;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.TcpClientProvider;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 *
 */
public class TcpUpdatesTaker implements UpdatesTaker {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final TcpClientProvider clientProvider;
    private final IndexReplicationProtocol indexReplicationProtocol;

    public TcpUpdatesTaker(TcpClientProvider clientProvider, IndexReplicationProtocol indexReplicationProtocol) {
        this.clientProvider = clientProvider;
        this.indexReplicationProtocol = indexReplicationProtocol;
    }

    @Override
    public  void takeUpdates(RingHost ringHost, RegionName tableName,
            long transationId, final WALScan tookStream) throws Exception {
        TcpClient client = clientProvider.getClientForHost(ringHost);
        try {
            TakeRowUpdatesRequestPayload requestPayload = new TakeRowUpdatesRequestPayload(tableName, transationId);
            Message message = new Message(indexReplicationProtocol.nextInteractionId(),
                    indexReplicationProtocol.OPCODE_REQUEST_CHANGESET, true, requestPayload);

            client.sendMessage(message);

            CallbackStream<RowUpdatesPayload> messageStream = new CallbackStream<RowUpdatesPayload>() {
                @Override
                public RowUpdatesPayload callback(RowUpdatesPayload changeSetPayload) throws Exception {
                    if (changeSetPayload != null) {
                        changeSetPayload.rowScan(tookStream);
                        return changeSetPayload;
                    } else {
                        return null;
                    }
                }
            };

            Message entry = null;
            boolean streamingResults = true;

            while ((entry = client.receiveMessage()) != null) {

                if (entry.getOpCode() == indexReplicationProtocol.OPCODE_ERROR) {
                    String errorMsg = entry.getPayload();
                    throw new Exception(errorMsg);
                }

                //if we aren't dispatching results anymore, we still need to loop over the input to drain the socket
                if (streamingResults) {
                    try {
                        RowUpdatesPayload payload = entry.getPayload();
                        RowUpdatesPayload returned = messageStream.callback(payload);
                        if (returned == null) {
                            streamingResults = false;
                        }
                    } catch (Exception ex) {
                        LOG.error("Error streaming in transaction set: " + entry, ex);
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
