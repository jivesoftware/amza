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

import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.shared.AmzaInstance;
import com.jivesoftware.os.amza.shared.stats.AmzaStats;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.server.ChunkedOutput;
import org.xerial.snappy.SnappyOutputStream;

@Singleton
@Path("/amza")
public class AmzaReplicationRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaStats amzaStats;
    private final AmzaInstance amzaInstance;

    public AmzaReplicationRestEndpoints(@Context AmzaStats amzaStats,
        @Context AmzaInstance amzaInstance) {
        this.amzaStats = amzaStats;
        this.amzaInstance = amzaInstance;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/rows/stream/{ringMemberString}/{versionedPartitionName}/{txId}/{leadershipToken}")
    public Response rowsStream(@PathParam("ringMemberString") String ringMemberString,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        @PathParam("txId") long txId,
        @PathParam("leadershipToken") long leadershipToken) {

        try {
            amzaStats.rowsStream.incrementAndGet();

            StreamingOutput stream = (OutputStream os) -> {
                os.flush();
                BufferedOutputStream bos = new BufferedOutputStream(os, 8192); // TODO expose to config
                final DataOutputStream dos = new DataOutputStream(new SnappyOutputStream(bos));
                try {
                    amzaInstance.rowsStream(dos,
                        new RingMember(ringMemberString),
                        VersionedPartitionName.fromBase64(versionedPartitionName),
                        txId,
                        leadershipToken);
                } catch (Exception x) {
                    LOG.error("Failed to stream takes.", x);
                    throw new IOException("Failed to stream takes.", x);
                } finally {
                    dos.flush();
                    amzaStats.rowsStream.decrementAndGet();
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
    public ChunkedOutput<byte[]> availableRowsStream(@PathParam("ringMember") String ringMemberString,
        @PathParam("takeSessionId") long takeSessionId,
        @PathParam("timeoutMillis") long timeoutMillis) {
        try {
            amzaStats.availableRowsStream.incrementAndGet();
            ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);

            new Thread(() -> {
                try {
                    amzaInstance.availableRowsStream(
                        chunkedOutput::write,
                        new RingMember(ringMemberString),
                        takeSessionId,
                        timeoutMillis);
                } catch (Exception x) {
                    LOG.warn("Failed to stream available rows", x);
                } finally {
                    try {
                        chunkedOutput.close();
                    } catch (IOException x) {
                        LOG.warn("Failed to close stream for available rows", x);
                    }
                }
            }, "available-" + ringMemberString).start();

            return chunkedOutput;
        } finally {
            amzaStats.availableRowsStream.decrementAndGet();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/rows/taken/{memberName}/{versionedPartitionName}/{txId}/{leadershipToken}")
    public Response rowsTaken(@PathParam("memberName") String ringMemberName,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        @PathParam("txId") long txId,
        @PathParam("leadershipToken") long leadershipToken) {
        try {
            amzaStats.rowsTaken.incrementAndGet();
            amzaInstance.rowsTaken(new RingMember(ringMemberName),
                VersionedPartitionName.fromBase64(versionedPartitionName),
                txId,
                leadershipToken);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to ack for member:{} partition:{} txId:{}",
                new Object[] { ringMemberName, versionedPartitionName, txId }, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to ack.", x);
        } finally {
            amzaStats.rowsTaken.decrementAndGet();
        }
    }

}
