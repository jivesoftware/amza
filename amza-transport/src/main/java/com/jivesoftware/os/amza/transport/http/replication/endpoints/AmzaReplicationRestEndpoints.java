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
import com.jivesoftware.os.amza.shared.ring.AmzaRing;
import com.jivesoftware.os.amza.shared.ring.RingHost;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.take.UpdatesTaker;
import com.jivesoftware.os.amza.transport.http.replication.TakeRequest;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
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
    private final AmzaRing amzaRing;
    private final AmzaInstance amzaInstance;

    public AmzaReplicationRestEndpoints(@Context AmzaRing amzaRing,
        @Context AmzaInstance amzaInstance) {
        this.amzaRing = amzaRing;
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
            amzaRing.register(ringMember, new RingHost(host, port));
            amzaRing.addRingMember("system", ringMember);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to add {}/{}/{} ", new Object[]{logicalName, host, port}, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to add system member: " + logicalName, x);
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/ring/remove/{logicalName}")
    public Response removeMember(@PathParam("logicalName") String logicalName) {
        try {
            LOG.info("Attempting to remove RingHost: " + logicalName);
            amzaRing.removeRingMember("system", new RingMember(logicalName));
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
            NavigableMap<RingMember, RingHost> ring = amzaRing.getRing("system");
            return ResponseHelper.INSTANCE.jsonResponse(ring);
        } catch (Exception x) {
            LOG.warn("Failed to get amza ring.", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to get amza ring.", x);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/changes/streamingTake")
    public Response streamingTake(final TakeRequest takeRequest) {
        try {

            StreamingOutput stream = (OutputStream os) -> {
                os.flush();
                BufferedOutputStream bos = new BufferedOutputStream(os, 8192); // TODO expose to config
                final DataOutputStream dos = new DataOutputStream(bos);
                try {
                    amzaInstance.streamingTakeFromPartition(dos,
                        takeRequest.getTaker(),
                        takeRequest.getTakerHost(),
                        takeRequest.getPartitionName(),
                        takeRequest.getHighestTransactionId());
                } catch (Exception x) {
                    LOG.error("Failed to stream takes.", x);
                    throw new IOException("Failed to stream takes.", x);
                } finally {
                    dos.flush();
                }
            };
            return Response.ok(stream).build();
        } catch (Exception x) {
            LOG.warn("Failed to apply changeset: " + takeRequest, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to changeset " + takeRequest, x);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/changes/acked/{logicalName}/{host}/{port}")
    public Response acks(@PathParam("logicalName") String ringMemberName,
        @PathParam("host") String ringHostName,
        @PathParam("port") int ringHostPort,
        List<UpdatesTaker.AckTaken> acks) {
        try {
            amzaInstance.takeAcks(new RingMember(ringMemberName), new RingHost(ringHostName, ringHostPort), (AmzaInstance.AcksStream acksStream) -> {
                for (UpdatesTaker.AckTaken ack : acks) {
                    acksStream.stream(ack.partitionName, ack.txId);
                }
            });
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to acked: " + acks, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to acked " + acks, x);
        }
    }

}
