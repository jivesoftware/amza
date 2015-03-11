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
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ApplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Response;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ResponseWriter;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.lang.mutable.MutableLong;

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
    private final NavigableMap<RowIndexKey, RowIndexValue> empty = new TreeMap<>();

    public IndexReplicationProtocol(AmzaInstance amzaInstance, OrderIdProvider idProvider) {
        this.amzaInstance = amzaInstance;
        this.idProvider = idProvider;

        Map<Integer, Class<? extends Serializable>> map = new HashMap<>();
        map.put(OPCODE_PUSH_CHANGESET, RowUpdatesPayload.class);
        map.put(OPCODE_REQUEST_CHANGESET, TakeRowUpdatesRequestPayload.class);
        map.put(OPCODE_RESPOND_CHANGESET, RowUpdatesPayload.class);
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
            RowUpdatesPayload payload = request.getPayload();
            amzaInstance.updates(payload.getMapName(), payload);

            final Message response = new Message(request.getInteractionId(), OPCODE_OK, true);
            LOG.trace("Returning from change set push {}", response);

            return messageResponse(response);

        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + request, x);
            ExceptionPayload exceptionPayload = new ExceptionPayload(x.toString());
            return messageResponse(new Message(request.getInteractionId(), OPCODE_ERROR, true, exceptionPayload));
        }
    }


    private Response handleChangeSetRequest(Message request) {

        LOG.trace("Received change set request {}", request);

        TakeRowUpdatesRequestPayload changeSet = request.getPayload();

        return streamChangeSetResponse(request.getInteractionId(), changeSet.getMapName(), changeSet.getHighestTransactionId());
    }

    private void streamChangeSet(final ResponseWriter responseWriter, final long interactionId,
            final TableName mapName, final long highestTransactionId) throws IOException {
        try {
            final int batchSize = 10; // TODO expose to config;
            final List<RowIndexKey> keys = new ArrayList<>();
            final List<RowIndexValue> values = new ArrayList<>();
            final MutableLong highestId = new MutableLong(-1);
            amzaInstance.takeRowUpdates(mapName, highestTransactionId, new RowScan() {
                @Override
                public boolean row(long orderId, RowIndexKey key, RowIndexValue value) throws Exception {
                    keys.add(key);
                    // We make this copy because we don't know how the value is being stored. By calling value.getValue()
                    // we ensure that the value from the tableIndex is real vs a pointer.
                    RowIndexValue copy = new RowIndexValue(value.getValue(), value.getTimestampId(), value.getTombstoned());
                    values.add(copy);
                    if (highestId.longValue() < orderId) {
                        highestId.setValue(orderId);
                    }
                    if (keys.size() > batchSize) {
                        RowUpdatesPayload sendChangeSetPayload = new RowUpdatesPayload(mapName, highestId.longValue(), keys, values);
                        Message responseMsg = new Message(interactionId, OPCODE_RESPOND_CHANGESET, false, sendChangeSetPayload);
                        LOG.trace("Writing response from change set request {}", responseMsg);
                        responseWriter.writeMessage(responseMsg);
                        keys.clear();
                        values.clear();
                    }
                    return true;
                }
            });

            RowUpdatesPayload sendChangeSetPayload = new RowUpdatesPayload(mapName, highestId.longValue(), keys, values);
            Message responseMsg = new Message(interactionId, OPCODE_RESPOND_CHANGESET, true, sendChangeSetPayload);
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
