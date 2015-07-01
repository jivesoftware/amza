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
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.NavigableMap;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/amza")
public class AmzaReplicationRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRingWriter ringWriter;
    private final AmzaRingReader ringReader;
    private final AmzaInstance amzaInstance;

    public AmzaReplicationRestEndpoints(@Context AmzaRingWriter ringWriter,
        @Context AmzaRingReader ringReader,
        @Context AmzaInstance amzaInstance) {
        this.ringWriter = ringWriter;
        this.ringReader = ringReader;
        this.amzaInstance = amzaInstance;
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/add/{logicalName}/{host}/{port}")
    public Response addMember(@PathParam("logicalName") String logicalName,
        @PathParam("host") String host,
        @PathParam("port") int port) {
        try {
            LOG.info("Attempting to add {}/{}/{} ", logicalName, host, port);
            RingMember ringMember = new RingMember(logicalName);
            ringWriter.register(ringMember, new RingHost(host, port));
            ringWriter.addRingMember(AmzaRingReader.SYSTEM_RING, ringMember);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to add {}/{}/{} ", new Object[] { logicalName, host, port }, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to add system member: " + logicalName, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/remove/{logicalName}")
    public Response removeMember(@PathParam("logicalName") String logicalName) {
        try {
            LOG.info("Attempting to remove RingHost: " + logicalName);
            ringWriter.removeRingMember(AmzaRingReader.SYSTEM_RING, new RingMember(logicalName));
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to add RingHost: " + logicalName, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to remove RingHost: " + logicalName, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/ring")
    public Response getRing() {
        try {
            LOG.info("Attempting to get amza ring.");
            NavigableMap<RingMember, RingHost> ring = ringReader.getRing(AmzaRingReader.SYSTEM_RING);
            return ResponseHelper.INSTANCE.jsonResponse(ring);
        } catch (Exception x) {
            LOG.warn("Failed to get amza ring.", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get amza ring.", x);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/rows/stream/{ringMemberString}/{versionedPartitionName}/{txId}")
    public Response rowsStream(@PathParam("ringMemberString") String ringMemberString,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        @PathParam("txId") long txId) {

        try {

            StreamingOutput stream = (OutputStream os) -> {
                os.flush();
                BufferedOutputStream bos = new BufferedOutputStream(os, 8192); // TODO expose to config
                final DataOutputStream dos = new DataOutputStream(bos);
                try {
                    amzaInstance.rowsStream(dos,
                        new RingMember(ringMemberString),
                        VersionedPartitionName.fromBase64(versionedPartitionName),
                        txId);
                } catch (Exception x) {
                    LOG.error("Failed to stream takes.", x);
                    throw new IOException("Failed to stream takes.", x);
                } finally {
                    dos.flush();
                }
            };
            return Response.ok(stream).build();
        } catch (Exception x) {
            Object[] vals = new Object[] { ringMemberString, versionedPartitionName, txId };
            LOG.warn("Failed to rowsStream {} {} {}. ", vals, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to rowsStream " + Arrays.toString(vals), x);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/rows/available/{ringMember}/{takeSessionId}/{timeoutMillis}")
    public Response availableRowsStream(@PathParam("ringMember") String ringMemberString,
        @PathParam("takeSessionId") long takeSessionId,
        @PathParam("timeoutMillis") long timeoutMillis) {
        try {

            StreamingOutput stream = (OutputStream os) -> {
                os.flush();
                final DataOutputStream dos = new DataOutputStream(os);
                try {
                    amzaInstance.availableRowsStream(dos, new RingMember(ringMemberString), takeSessionId, timeoutMillis);
                } catch (Exception x) {
                    LOG.error("Failed to stream takes.", x);
                    throw new IOException("Failed to stream takes.", x);
                } finally {
                    dos.flush();
                }
            };
            return Response.ok(stream).build();
        } catch (Exception x) {
            LOG.warn("Failed to stream partition updates. ", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to stream partition updates.", x);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/rows/taken/{memberName}/{versionedPartitionName}/{txId}")
    public Response rowsTaken(@PathParam("memberName") String ringMemberName,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        @PathParam("txId") long txId) {
        try {
            amzaInstance.rowsTaken(new RingMember(ringMemberName),
                VersionedPartitionName.fromBase64(versionedPartitionName),
                txId);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to ack for member:{} partition:{} txId:{}",
                new Object[] { ringMemberName, versionedPartitionName, txId }, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to ack.", x);
        }
    }

}
