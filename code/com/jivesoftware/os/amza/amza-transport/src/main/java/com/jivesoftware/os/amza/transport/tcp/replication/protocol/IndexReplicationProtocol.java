package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.ApplicationProtocol;
import com.jivesoftware.os.amza.transport.tcp.replication.shared.Message;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
    private final Map<Integer, Class<? extends Serializable>> payloadRegistry;
    private final AmzaInstance amzaInstance;
    private final OrderIdProvider idProvider;

    public IndexReplicationProtocol(AmzaInstance amzaInstance, OrderIdProvider idProvider) {
        this.amzaInstance = amzaInstance;
        this.idProvider = idProvider;

        Map<Integer, Class<? extends Serializable>> map = new HashMap<>();
        map.put(OPCODE_PUSH_CHANGESET, SendChangeSetPayload.class);
        map.put(OPCODE_RESPOND_CHANGESET, ChangeSetResponsePayload.class);
        map.put(OPCODE_ERROR, String.class);
        payloadRegistry = Collections.unmodifiableMap(map);
    }

    @Override
    public Message handleRequest(Message request) {
        switch (request.getOpCode()) {
            case OPCODE_PUSH_CHANGESET:
                return handleChangeSetPush(request);
            case OPCODE_REQUEST_CHANGESET:
                return handleChangeSetRequest(request);
            default:
                throw new IllegalArgumentException("Unexpected opcode: " + request.getOpCode());
        }
    }

    private Message handleChangeSetPush(Message request) {
        try {
            SendChangeSetPayload payload = request.getPayload();
            amzaInstance.changes(payload.getMapName(), changeSetToPartionDelta(payload));
            return null;
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + request, x);
            ExceptionPayload exceptionPayload = new ExceptionPayload(x.toString());
            return new Message(request.getInteractionId(), OPCODE_ERROR, true, exceptionPayload);
        }
    }

    private <K, V> TableDelta<K, V> changeSetToPartionDelta(SendChangeSetPayload changeSet) throws Exception {
        final ConcurrentNavigableMap<K, TimestampedValue<V>> changes = new ConcurrentSkipListMap<>();
        changes.putAll(changeSet.getChanges());
        return new TableDelta<>(changes, new TreeMap(), null);
    }

    //TODO figure out how to stream this out in stages vi calls to consumeSequence.
    private <K, V> Message handleChangeSetRequest(Message request) {
        try {

            ChangeSetRequestPayload changeSet = request.getPayload();

            final ConcurrentNavigableMap<K, TimestampedValue<V>> changes = new ConcurrentSkipListMap<>();

            final MutableLong highestTransactionId = new MutableLong();

            amzaInstance.takeTableChanges(changeSet.getMapName(), changeSet.getHighestTransactionId(), new TransactionSetStream<K, V>() {
                @Override
                public boolean stream(TransactionSet<K, V> took) throws Exception {
                    changes.putAll(took.getChanges());

                    if (took.getHighestTransactionId() > highestTransactionId.longValue()) {
                        highestTransactionId.setValue(took.getHighestTransactionId());
                    }
                    return true;
                }
            });

            ChangeSetResponsePayload response = new ChangeSetResponsePayload(new TransactionSet(highestTransactionId.longValue(), changes));

            return new Message(request.getInteractionId(), OPCODE_RESPOND_CHANGESET, true, response);
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + request, x);
            ExceptionPayload exceptionPayload = new ExceptionPayload(x.toString());
            return new Message(request.getInteractionId(), OPCODE_ERROR, true, exceptionPayload);
        }
    }

    @Override
    public Message consumeSequence(long interactionId) {
        throw new UnsupportedOperationException("Sequences not supported yet - currently sending whole changesets");
    }

    @Override
    public Class<? extends Serializable> getOperationPayloadClass(int opCode) {
        return payloadRegistry.get(opCode);
    }

    @Override
    public long nextInteractionId() {
        return idProvider.nextId();
    }
}
