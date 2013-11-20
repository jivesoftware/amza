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
package com.jivesoftware.os.amza.transport.http.replication.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.TableDelta;
import com.jivesoftware.os.amza.shared.TableRowReader;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.TransactionSet;
import com.jivesoftware.os.amza.shared.TransactionSetStream;
import com.jivesoftware.os.amza.storage.TransactionEntry;
import com.jivesoftware.os.amza.storage.json.StringRowMarshaller;
import com.jivesoftware.os.amza.transport.http.replication.ChangeSet;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.mutable.MutableLong;

@Path("/amza")
public class AmzaReplicationRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaInstance amzaInstance;

    public AmzaReplicationRestEndpoints(@Context AmzaInstance amzaInstance) {
        this.amzaInstance = amzaInstance;
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/add")
    public Response addHost(final RingHost ringHost) {
        try {
            LOG.info("Attempting to add RingHost: " + ringHost);
            amzaInstance.addRingHost("master", ringHost);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to add RingHost: " + ringHost, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to add RingHost: " + ringHost, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/remove")
    public Response removeHost(final RingHost ringHost) {
        try {
            LOG.info("Attempting to remove RingHost: " + ringHost);
            amzaInstance.removeRingHost("master", ringHost);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to add RingHost: " + ringHost, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove RingHost: " + ringHost, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/ring")
    public Response getRing() {
        try {
            LOG.info("Attempting to get amza ring.");
            List<RingHost> ring = amzaInstance.getRing("master");
            return ResponseHelper.INSTANCE.jsonResponse(ring);
        } catch (Exception x) {
            LOG.warn("Failed to get amza ring.", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get amza ring.", x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/changes/add")
    public Response changeset(final ChangeSet changeSet) {
        try {
            amzaInstance.changes(changeSet.getTableName(), changeSetToPartionDelta(changeSet));
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + changeSet, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to changeset " + changeSet, x);
        }
    }

    private <K, V> TableDelta<K, V> changeSetToPartionDelta(ChangeSet changeSet) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TableRowReader<String> rowReader = new StringArrayRowReader(changeSet.getChanges());
        final StringRowMarshaller<K, V> jsonPartitionRowMarshaller = new StringRowMarshaller<>(mapper, changeSet.getTableName());

        final ConcurrentNavigableMap<K, TimestampedValue<V>> changes = new ConcurrentSkipListMap<>();
        rowReader.read(false, new TableRowReader.Stream<String>() {
            @Override
            public boolean stream(String kvt) throws Exception {
                TransactionEntry<K, V> te = jsonPartitionRowMarshaller.fromRow(kvt);
                changes.put(te.getKey(), te.getValue());
                return true;
            }
        });

        return new TableDelta<>(changes, new TreeMap(), null);
    }

    @POST
    @Consumes("application/json")
    @Path("/changes/take")
    public Response take(final ChangeSet changeSet) {
        try {

            final StringRowMarshaller jsonPartitionRowMarshaller = new StringRowMarshaller(new ObjectMapper(), changeSet.getTableName());
            final List<String> rows = new ArrayList<>();
            final MutableLong highestTransactionId = new MutableLong();
            amzaInstance.takeTableChanges(changeSet.getTableName(), changeSet.getHighestTransactionId(), new TransactionSetStream<Object, Object>() {

                @Override
                public boolean stream(TransactionSet<Object, Object> took) throws Exception {
                    NavigableMap<Object, TimestampedValue<Object>> changes = took.getChanges();
                    for (Map.Entry<Object, TimestampedValue<Object>> e : changes.entrySet()) {
                        rows.add(jsonPartitionRowMarshaller.toRow(0, e));
                    }
                    if (took.getHighestTransactionId() > highestTransactionId.longValue()) {
                        highestTransactionId.setValue(took.getHighestTransactionId());
                    }
                    return true;
                }
            });

            return ResponseHelper.INSTANCE.jsonResponse(new ChangeSet(highestTransactionId.longValue(), changeSet.getTableName(), rows));
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + changeSet, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to changeset " + changeSet, x);
        }
    }
}