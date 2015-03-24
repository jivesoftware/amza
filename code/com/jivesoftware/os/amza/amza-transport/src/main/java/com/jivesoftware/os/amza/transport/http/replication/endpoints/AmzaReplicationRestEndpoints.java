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

import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.AmzaRing;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALScan;
import com.jivesoftware.os.amza.shared.WALScanable;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.RowMarshaller;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.transport.http.replication.RowUpdates;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.mutable.MutableLong;

@Path("/amza")
public class AmzaReplicationRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRing amzaRing;
    private final AmzaInstance amzaInstance;

    public AmzaReplicationRestEndpoints(@Context AmzaRing amzaRing,
        @Context AmzaInstance amzaInstance) {
        this.amzaRing = amzaRing;
        this.amzaInstance = amzaInstance;
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/add")
    public Response addHost(final RingHost ringHost) {
        try {
            LOG.info("Attempting to add RingHost: " + ringHost);
            amzaRing.addRingHost("master", ringHost);
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
            amzaRing.removeRingHost("master", ringHost);
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
            List<RingHost> ring = amzaRing.getRing("master");
            return ResponseHelper.INSTANCE.jsonResponse(ring);
        } catch (Exception x) {
            LOG.warn("Failed to get amza ring.", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get amza ring.", x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/tables")
    public Response getTables() {
        try {
            LOG.info("Attempting to get table names.");
            List<RegionName> tableNames = amzaInstance.getRegionNames();
            return ResponseHelper.INSTANCE.jsonResponse(tableNames);
        } catch (Exception x) {
            LOG.warn("Failed to get table names.", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get table names.", x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/changes/add")
    public Response changeset(final RowUpdates changeSet) {
        try {
            amzaInstance.updates(changeSet.getRegionName(), changeSetToScanable(changeSet));
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + changeSet, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to changeset " + changeSet, x);
        }
    }

    private WALScanable changeSetToScanable(final RowUpdates changeSet) throws Exception {

        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        return new WALScanable() {
            @Override
            public <E extends Exception> void rowScan(WALScan<E> rowScan) throws Exception {
                for (byte[] row : changeSet.getChanges()) {
                    RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                    if (!rowScan.row(walr.getTransactionId(), walr.getKey(), walr.getValue())) {
                        return;
                    }
                }
            }

            @Override
            public <E extends Exception> void rangeScan(WALKey from, WALKey to, WALScan<E> rowScan) throws Exception {
                for (byte[] row : changeSet.getChanges()) {
                    RowMarshaller.WALRow walr = rowMarshaller.fromRow(row);
                    if (!rowScan.row(walr.getTransactionId(), walr.getKey(), walr.getValue())) {
                        return;
                    }
                }
            }

        };
    }

    @POST
    @Consumes("application/json")
    @Path("/changes/take")
    public Response take(final RowUpdates rowUpdates) {
        try {

            final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
            final List<byte[]> rows = new ArrayList<>();
            final MutableLong highestTransactionId = new MutableLong();
            amzaInstance.takeRowUpdates(rowUpdates.getRegionName(), rowUpdates.getHighestTransactionId(), new WALScan() {
                @Override
                public boolean row(long orderId, WALKey key, WALValue value) throws Exception {
                    rows.add(rowMarshaller.toRow(orderId, key, value));
                    if (orderId > highestTransactionId.longValue()) {
                        highestTransactionId.setValue(orderId);
                    }
                    return true;
                }
            });

            return ResponseHelper.INSTANCE.jsonResponse(new RowUpdates(highestTransactionId.longValue(), rowUpdates.getRegionName(), rows));
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + rowUpdates, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to changeset " + rowUpdates, x);
        }
    }
}
