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
package com.jivesoftware.os.amza.service.replication.http.endpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.service.AmzaInstance;
import com.jivesoftware.os.amza.service.stats.AmzaStats;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
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
import org.glassfish.jersey.server.LatchChunkedOutput;
import org.nustaq.serialization.FSTConfiguration;
import org.xerial.snappy.SnappyOutputStream;

@Singleton
@Path("/amza")
public class AmzaReplicationRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    private final AmzaStats amzaStats;
    private final AmzaInstance amzaInstance;
    private final AmzaInterner amzaInterner;
    private final ObjectMapper objectMapper;

    public AmzaReplicationRestEndpoints(@Context AmzaStats amzaStats,
        @Context AmzaInstance amzaInstance,
        @Context AmzaInterner amzaInterner,
        @Context ObjectMapper objectMapper) {
        this.amzaStats = amzaStats;
        this.amzaInstance = amzaInstance;
        this.amzaInterner = amzaInterner;
        this.objectMapper = objectMapper;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/rows/stream/{ringMemberString}/{versionedPartitionName}/{takeSessionId}/{txId}/{leadershipToken}/{limit}")
    public Response rowsStream(@PathParam("ringMemberString") String ringMemberString,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        @PathParam("takeSessionId") long takeSessionId,
        @PathParam("txId") long txId,
        @PathParam("leadershipToken") long leadershipToken,
        @PathParam("limit") long limit,
        byte[] takeSharedKey) {

        try {
            amzaStats.rowsStream.increment();

            StreamingOutput stream = (OutputStream os) -> {
                os.flush();
                BufferedOutputStream bos = new BufferedOutputStream(new SnappyOutputStream(os), 8192); // TODO expose to config
                final DataOutputStream dos = new DataOutputStream(bos);
                try {
                    amzaInstance.rowsStream(dos,
                        new RingMember(ringMemberString),
                        VersionedPartitionName.fromBase64(versionedPartitionName, amzaInterner),
                        takeSessionId,
                        objectMapper.readValue(takeSharedKey, Long.class),
                        txId,
                        leadershipToken,
                        limit);
                } catch (IOException x) {
                    if (x.getCause() instanceof TimeoutException) {
                        LOG.error("Timed out while streaming takes");
                    } else {
                        LOG.error("Failed to stream takes.", x);
                    }
                    throw x;
                } catch (Exception x) {
                    LOG.error("Failed to stream takes.", x);
                    throw new IOException("Failed to stream takes.", x);
                } finally {
                    dos.flush();
                    amzaStats.rowsStream.decrement();
                    amzaStats.completedRowsStream.increment();
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
    @Path("/rows/available/{ringMember}/{ringHost}/{system}/{ringTimestampId}/{takeSessionId}/{timeoutMillis}")
    public ChunkedOutput<byte[]> availableRowsStream(@PathParam("ringMember") String ringMemberString,
        @PathParam("ringHost") String ringHost,
        @PathParam("system") boolean system,
        @PathParam("ringTimestampId") long ringTimestampId,
        @PathParam("takeSessionId") long takeSessionId,
        @PathParam("timeoutMillis") long timeoutMillis,
        byte[] sharedKey) {

        LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
        new Thread(() -> {
            chunkedOutput.await("availableRowsStream", () -> {
                amzaStats.availableRowsStream.increment();
                try {
                    amzaInstance.availableRowsStream(
                        system,
                        chunkedOutput::write,
                        new RingMember(ringMemberString),
                        new TimestampedRingHost(RingHost.fromCanonicalString(ringHost), ringTimestampId),
                        takeSessionId,
                        objectMapper.readValue(sharedKey, Long.class),
                        timeoutMillis);
                    return null;
                } finally {
                    amzaStats.availableRowsStream.decrement();
                }
            });
        }, "available-" + ringMemberString + "-" + (system ? "system" : "striped")).start();
        return chunkedOutput;
    }

    /*@POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/rows/taken/{memberName}/{takeSessionId}/{versionedPartitionName}/{txId}/{leadershipToken}")
    public Response rowsTaken(@PathParam("memberName") String ringMemberName,
        @PathParam("takeSessionId") long takeSessionId,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        @PathParam("txId") long txId,
        @PathParam("leadershipToken") long leadershipToken,
        byte[] sharedKey) {
        try {
            amzaStats.rowsTaken.increment();
            amzaInstance.rowsTaken(new RingMember(ringMemberName),
                takeSessionId,
                new String(sharedKey, StandardCharsets.UTF_8),
                VersionedPartitionName.fromBase64(versionedPartitionName, amzaInterner),
                txId,
                leadershipToken);
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed to ack for member:{} partition:{} txId:{}",
                new Object[] { ringMemberName, versionedPartitionName, txId }, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to ack.", x);
        } finally {
            amzaStats.rowsTaken.decrement();
            amzaStats.completedRowsTake.increment();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/pong/{memberName}/{takeSessionId}")
    public Response pong(@PathParam("memberName") String ringMemberName,
        @PathParam("takeSessionId") long takeSessionId,
        byte[] sharedKey) {
        try {
            amzaInstance.pong(new RingMember(ringMemberName), takeSessionId, new String(sharedKey, StandardCharsets.UTF_8));
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed pong for member:{} session:{}", new Object[] { ringMemberName, takeSessionId }, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed pong.", x);
        } finally {
            amzaStats.pongsReceived.increment();
        }
    }*/

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ackBatch")
    public Response ackBatch(InputStream is) {
        try {

            DataInputStream in = new DataInputStream(is);
            try {

                while (in.readByte() == 1) {

                    int length = in.readShort();
                    byte[] bytes = new byte[length];
                    in.readFully(bytes);
                    VersionedPartitionName versionedPartitionName = amzaInterner.internVersionedPartitionName(bytes, 0, length);

                    length = in.readShort();
                    bytes = new byte[length];
                    in.readFully(bytes);
                    RingMember ringMember = amzaInterner.internRingMember(bytes, 0, length);


                    long takeSessionId = in.readLong();
                    long takeSharedKey = in.readLong();
                    long txId = in.readLong();
                    long leadershipToken = in.readLong();

                    amzaInstance.rowsTaken(ringMember,
                        takeSessionId,
                        takeSharedKey,
                        versionedPartitionName,
                        txId,
                        leadershipToken);

                }

                if (in.readByte() == 1) {
                    int length = in.readShort();
                    byte[] bytes = new byte[length];
                    in.readFully(bytes);
                    RingMember ringMember = amzaInterner.internRingMember(bytes, 0, length);

                    long takeSessionId = in.readLong();
                    long takeSharedKey = in.readLong();

                    amzaInstance.pong(ringMember,
                        takeSessionId,
                        takeSharedKey);
                }

                return Response.ok(conf.asByteArray(Boolean.TRUE)).build();

            } finally {
                try {
                    in.close();
                } catch (Exception x) {
                    LOG.error("Failed to close input stream", x);
                }
            }

        } catch (Exception x) {
            LOG.warn("Failed ackBatch", x);
            return ResponseHelper.INSTANCE.errorResponse("Failed ackBatch.", x);
        } finally {
            amzaStats.pongsReceived.increment();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/invalidate/{memberName}/{takeSessionId}/{versionedPartitionName}")
    public Response invalidate(@PathParam("memberName") String ringMemberName,
        @PathParam("takeSessionId") long takeSessionId,
        @PathParam("versionedPartitionName") String versionedPartitionName,
        byte[] sharedKey) {
        try {
            amzaInstance.invalidate(new RingMember(ringMemberName),
                takeSessionId,
                UIO.bytesLong(sharedKey),
                VersionedPartitionName.fromBase64(versionedPartitionName, amzaInterner));
            return ResponseHelper.INSTANCE.jsonResponse(Boolean.TRUE);
        } catch (Exception x) {
            LOG.warn("Failed invalidate for member:{} session:{} partition:{}", new Object[] { ringMemberName, takeSessionId, versionedPartitionName }, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed pong.", x);
        } finally {
            amzaStats.pongsReceived.increment();
        }
    }

}
