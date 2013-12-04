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
package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.BinaryTimestampedValue;
import com.jivesoftware.os.amza.shared.EntryStream;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableIndex;
import com.jivesoftware.os.amza.shared.TableIndexKey;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ApplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Response;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ResponseWriter;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public class IndexReplicationProtocol implements ApplicationProtocol {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    public final int OPCODE_PUSH_CHANGESET = 3;
    public final int OPCODE_REQUEST_CHANGESET = 5;
    public final int OPCODE_RESPOND_CHANGESET = 7;
    public final int OPCODE_ERROR = 9;
    public final int OPCODE_OK = 11;
    private final Map<Integer, Class<? extends Serializable>> payloadRegistry;
    private final AmzaInstance amzaInstance;
    private final OrderIdProvider idProvider;
    private final NavigableMap<TableIndexKey, BinaryTimestampedValue> empty = new TreeMap<>();

    public IndexReplicationProtocol(AmzaInstance amzaInstance, OrderIdProvider idProvider) {
        this.amzaInstance = amzaInstance;
        this.idProvider = idProvider;

        Map<Integer, Class<? extends Serializable>> map = new HashMap<>();
        map.put(OPCODE_PUSH_CHANGESET, SendChangeSetPayload.class);
        map.put(OPCODE_REQUEST_CHANGESET, ChangeSetRequestPayload.class);
        map.put(OPCODE_RESPOND_CHANGESET, ChangeSetResponsePayload.class);
        map.put(OPCODE_ERROR, String.class);
        payloadRegistry = Collections.unmodifiableMap(map);
    }

    @Override
    public Response handleRequest(Message request) {
        switch (request.getOpCode()) {
            case OPCODE_PUSH_CHANGESET:
                return handleChangeSetPush(request);
            case OPCODE_REQUEST_CHANGESET:
                return handleChangeSetRequest(request);
            default:
                throw new IllegalArgumentException("Unexpected opcode: " + request.getOpCode());
        }
    }

    private Response handleChangeSetPush(Message request) {
        LOG.trace("Received change set push {}", request);

        try {
            SendChangeSetPayload payload = request.getPayload();
            amzaInstance.changes(payload.getMapName(), changeSetToPartionDelta(payload));

            final Message response = new Message(request.getInteractionId(), OPCODE_OK, true);
            LOG.trace("Returning from change set push {}", response);

            return messageResponse(response);

        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + request, x);
            ExceptionPayload exceptionPayload = new ExceptionPayload(x.toString());
            return messageResponse(new Message(request.getInteractionId(), OPCODE_ERROR, true, exceptionPayload));
        }
    }

    private TableDelta changeSetToPartionDelta(SendChangeSetPayload changeSet) throws Exception {
        final ConcurrentNavigableMap<TableIndexKey, BinaryTimestampedValue> changes = new ConcurrentSkipListMap<>();
        TableIndex tableIndex = changeSet.getChanges();
        tableIndex.entrySet(new EntryStream<RuntimeException>() {
            @Override
            public boolean stream(TableIndexKey key, BinaryTimestampedValue value) {
                changes.put(key, value);
                return true;
            }
        });
        return new TableDelta(changes, new TreeMap(), null);
    }

    private Response handleChangeSetRequest(Message request) {

        LOG.trace("Received change set request {}", request);

        ChangeSetRequestPayload changeSet = request.getPayload();

        return streamChangeSetResponse(request.getInteractionId(), changeSet.getMapName(), changeSet.getHighestTransactionId());
    }

    private void streamChangeSet(final ResponseWriter responseWriter, final long interactionId,
            final TableName mapName, final long highestTransactionId) throws IOException {
        try {

            amzaInstance.takeTableChanges(mapName, highestTransactionId, new TransactionSetStream() {
                @Override
                public boolean stream(TransactionSet took) throws Exception {
                    ChangeSetResponsePayload response = new ChangeSetResponsePayload(took);
                    Message responseMsg = new Message(interactionId, OPCODE_RESPOND_CHANGESET, false, response);
                    LOG.trace("Writing response from change set request {}", responseMsg);
                    responseWriter.writeMessage(responseMsg);


                    return true;
                }
            });

            ChangeSetResponsePayload response = new ChangeSetResponsePayload(new TransactionSet(-1, empty));
            Message responseMsg = new Message(interactionId, OPCODE_RESPOND_CHANGESET, true, response);
            LOG.trace("Writing final response from change set request {}", responseMsg);
            responseWriter.writeMessage(responseMsg);



        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + mapName, x);
            ExceptionPayload exceptionPayload = new ExceptionPayload(x.toString());
            responseWriter.writeMessage(new Message(interactionId, OPCODE_ERROR, true, exceptionPayload));
        }
    }

    @Override
    public Message consumeSequence(long interactionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends Serializable> getOperationPayloadClass(int opCode) {
        return payloadRegistry.get(opCode);
    }

    @Override
    public long nextInteractionId() {
        return idProvider.nextId();
    }

    private Response messageResponse(final Message message) {
        return new Response() {
            @Override
            public Message getMessage() {
                return message;
            }

            @Override
            public boolean isBlocking() {
                return false;
            }

            @Override
            public boolean hasMessage() {
                return true;
            }

            @Override
            public void writeTo(ResponseWriter responseWriter) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Response streamChangeSetResponse(final long interactionId, final TableName mapName, final long highestTransactionId) {
        return new Response() {
            @Override
            public Message getMessage() {
                return null;
            }

            @Override
            public boolean isBlocking() {
                return true;
            }

            @Override
            public boolean hasMessage() {
                return false;
            }

            @Override
            public void writeTo(ResponseWriter responseWriter) throws IOException {
                streamChangeSet(responseWriter, interactionId, mapName, highestTransactionId);
            }
        };
    }
}
