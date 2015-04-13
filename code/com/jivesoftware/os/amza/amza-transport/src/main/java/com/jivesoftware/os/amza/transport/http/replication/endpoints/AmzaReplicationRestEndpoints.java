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
import com.jivesoftware.os.amza.shared.HighwaterMarks;
import com.jivesoftware.os.amza.shared.HostRing;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RingHost;
import com.jivesoftware.os.amza.shared.RowStream;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.Scannable;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.amza.storage.binary.BinaryRowMarshaller;
import com.jivesoftware.os.amza.transport.http.replication.RowUpdates;
import com.jivesoftware.os.amza.transport.http.replication.TakeRequest;
import com.jivesoftware.os.jive.utils.jaxrs.util.ResponseHelper;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.lang.mutable.MutableLong;

@Path("/amza")
public class AmzaReplicationRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRing amzaRing;
    private final AmzaInstance amzaInstance;
    private final HighwaterMarks highwaterMarks;

    public AmzaReplicationRestEndpoints(@Context AmzaRing amzaRing,
        @Context AmzaInstance amzaInstance,
        @Context HighwaterMarks highwaterMarks) {
        this.amzaRing = amzaRing;
        this.amzaInstance = amzaInstance;
        this.highwaterMarks = highwaterMarks;
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/add")
    public Response addHost(final RingHost ringHost) {
        try {
            LOG.info("Attempting to add RingHost: " + ringHost);
            amzaRing.addRingHost("system", ringHost);
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
            amzaRing.removeRingHost("system", ringHost);
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
            List<RingHost> ring = amzaRing.getRing("system");
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

    private Scannable<WALValue> changeSetToScanable(final RowUpdates changeSet) throws Exception {

        final BinaryRowMarshaller rowMarshaller = new BinaryRowMarshaller();
        return new Scannable<WALValue>() {
            @Override
            public void rowScan(Scan<WALValue> scan) throws Exception {
                changeSet.stream(rowMarshaller, scan);
            }
        };
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/changes/streamingTake")
    public Response streamingTake(final TakeRequest takeRequest) {
        try {

            final long t1 = System.currentTimeMillis();
            StreamingOutput stream = new StreamingOutput() {
                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    long t2 = System.currentTimeMillis();
                    os.flush();
                    long t3 = System.currentTimeMillis();
                    BufferedOutputStream bos = new BufferedOutputStream(os, 8192); // TODO expose to config
                    final DataOutputStream dos = new DataOutputStream(bos);
                    long t4 = -1, t5 = -1;
                    final MutableLong bytes = new MutableLong(0);
                    try {
                        RegionName regionName = takeRequest.getRegionName();
                        HostRing hostRing = amzaRing.getHostRing(regionName.getRingName());
                        for (RingHost ringHost : hostRing.getAboveRing()) {
                            Long highwatermark = highwaterMarks.get(ringHost, regionName);
                            if (highwatermark != null) {
                                byte[] ringHostBytes = ringHost.toBytes();
                                dos.writeByte(1);
                                dos.writeInt(ringHostBytes.length);
                                dos.write(ringHostBytes);
                                dos.writeLong(highwatermark);
                                bytes.add(1 + 4 + ringHostBytes.length + 8);
                            }
                        }
                        dos.writeByte(0); // last entry marker
                        t4 = System.currentTimeMillis();
                        bytes.increment();

                        amzaInstance.takeRowUpdates(regionName, takeRequest.getHighestTransactionId(), new RowStream() {
                            @Override
                            public boolean row(long rowFP, long rowTxId, byte rowType, byte[] row) throws Exception {
                                dos.writeByte(1);
                                dos.writeLong(rowTxId);
                                dos.writeByte(rowType);
                                dos.writeInt(row.length);
                                dos.write(row);
                                bytes.add(1 + 8 + 1 + 4 + row.length);
                                return true;
                            }
                        });
                        t5 = System.currentTimeMillis();
                        dos.writeByte(0); // last entry marker
                        bytes.increment();
                    } catch (Exception x) {
                        LOG.error("Failed to stream takes.", x);
                        throw new IOException("Failed to stream takes.", x);
                    } finally {
                        dos.flush();
                        long t6 = System.currentTimeMillis();
                        if (!takeRequest.getRegionName().isSystemRegion()) {
                            LOG.debug("Give {}: OutputStream={}ms FirstFlush={}ms HighWater={}ms RowUpdates={}ms FinalFlush={}ms TotalTime={}ms TotalBytes={}",
                                takeRequest.getRegionName().getRegionName(), (t2 - t1), (t3 - t2), (t4 - t3), (t5 - t4), (t6 - t5), (t6 - t1),
                                bytes.longValue());
                        }
                    }
                }
            };
            return Response.ok(stream).build();
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + takeRequest, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to changeset " + takeRequest, x);
        }
    }

}
