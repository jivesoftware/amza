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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.RingPartitionProperties;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.FilerOutputStream;
import com.jivesoftware.os.amza.api.filer.ICloseable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.wal.KeyUtil;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.service.NotARingMemberException;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.amza.service.replication.http.AmzaRestClient;
import com.jivesoftware.os.amza.service.replication.http.AmzaRestClient.RingLeader;
import com.jivesoftware.os.amza.service.replication.http.AmzaRestClient.StateMessageCause;
import com.jivesoftware.os.amza.service.ring.RingTopology;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.server.LatchChunkedOutput;
import org.xerial.snappy.SnappyOutputStream;

@Singleton
@Path("/amza/v1")
public class AmzaClientRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRestClient client;
    private final AmzaInterner amzaInterner;
    private final ExecutorService chunkExecutors = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("stream-chunks-%d").build());

    public AmzaClientRestEndpoints(@Context AmzaRestClient client,
        @Context AmzaInterner amzaInterner) {
        this.client = client;
        this.amzaInterner = amzaInterner;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/properties/{base64PartitionName}")
    public Response getProperties(@PathParam("base64PartitionName") String base64PartitionName) {
        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
            RingPartitionProperties properties = client.getProperties(partitionName);
            return Response.ok(properties).build();
        } catch (Exception e) {
            LOG.error("Failed while attempting to getProperties:{}", new Object[] { partitionName }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.INTERNAL_SERVER_ERROR, "Failed while attempting to getProperties.", e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/configPartition/{base64PartitionName}/{ringSize}")
    public Object configPartition(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("ringSize") int ringSize,
        PartitionProperties partitionProperties) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
            RingTopology ringTopology = client.configPartition(partitionName, partitionProperties, ringSize);
            LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
            chunkedOutput.submit(chunkExecutors, partitionName, "configPartition", null, 4096, (partitionName1, in, out) -> {
                client.configPartition(ringTopology, out);
            });
            return chunkedOutput;
        } catch (Exception e) {
            LOG.error("Failed while attempting to configPartition:{} {} {}", new Object[] { partitionName, partitionProperties, ringSize }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.INTERNAL_SERVER_ERROR, "Failed while attempting to configPartition.", e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/ensurePartition/{base64PartitionName}/{waitForLeaderElection}")
    public Object ensurePartition(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
            client.ensurePartition(partitionName, waitForLeaderElection);
            return Response.ok().build();
        } catch (TimeoutException e) {
            LOG.error("No leader elected within timeout:{} {} millis", new Object[] { partitionName, waitForLeaderElection }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.SERVICE_UNAVAILABLE, "No leader elected within timeout.", e);
        } catch (NotARingMemberException e) {
            LOG.warn("Not a ring member for {}", partitionName);
            return ResponseHelper.INSTANCE.errorResponse(Status.CONFLICT, "Not a ring member.", e);
        } catch (Exception e) {
            LOG.error("Failed while attempting to ensurePartition:{}", new Object[] { partitionName }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.INTERNAL_SERVER_ERROR, "Failed while attempting to ensurePartition.", e);
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ring/{base64PartitionName}")
    public Object ring(@PathParam("base64PartitionName") String base64PartitionName) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
            RingLeader ringLeader = client.ring(partitionName);
            StreamingOutput stream = os -> {
                os.flush();
                FilerOutputStream fos = new FilerOutputStream(new BufferedOutputStream(os, 8192));
                try {
                    client.ring(ringLeader, fos);
                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    fos.close();
                }
            };
            return Response.ok(stream).build();
        } catch (Exception e) {
            LOG.error("Failed while attempting to get ring:{}", new Object[] { partitionName }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.INTERNAL_SERVER_ERROR, "Failed while getting ring.", e);
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ringLeader/{base64PartitionName}/{waitForLeaderElection}")
    public Object ringLeader(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
            RingLeader ringLeader = client.ringLeader(partitionName, waitForLeaderElection);
            StreamingOutput stream = os -> {
                os.flush();
                FilerOutputStream fos = new FilerOutputStream(new BufferedOutputStream(os, 8192));
                try {
                    client.ring(ringLeader, fos);
                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    fos.close();
                }
            };
            return Response.ok(stream).build();
        } catch (TimeoutException e) {
            LOG.error("No leader elected within timeout:{} {} millis", new Object[] { partitionName, waitForLeaderElection }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.SERVICE_UNAVAILABLE, "No leader elected within timeout.", e);
        } catch (Exception e) {
            LOG.error("Failed while attempting to get ring:{}", new Object[] { partitionName }, e);
            return ResponseHelper.INSTANCE.errorResponse(Status.INTERNAL_SERVER_ERROR, "Failed while awaiting ring leader.", e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/commit/{base64PartitionName}/{consistency}/{checkLeader}")
    public Response commit(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        InputStream inputStream) {

        FilerInputStream in = null;
        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
            in = new FilerInputStream(inputStream);
            StateMessageCause stateMessageCause = client.commit(partitionName,
                Consistency.valueOf(consistencyName),
                checkLeader, 10_000, in);
            if (stateMessageCause != null) {
                return stateMessageCauseToResponse(stateMessageCause);
            }
            return Response.ok("success").build();

        } catch (DeltaOverCapacityException x) {
            LOG.warn("Delta over capacity for {} {}", base64PartitionName, x);
            return ResponseHelper.INSTANCE.errorResponse(Status.SERVICE_UNAVAILABLE, "Delta over capacity.");
        } catch (FailedToAchieveQuorumException x) {
            LOG.warn("FailedToAchieveQuorumException for {} {}", base64PartitionName, x);
            return ResponseHelper.INSTANCE.errorResponse(Status.ACCEPTED, "Failed to achieve quorum exception.");
        } catch (Exception x) {
            Object[] vals = new Object[] { partitionName, consistencyName };
            LOG.warn("Failed to commit to {} at {}.", vals, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to commit: " + Arrays.toString(vals), x);
        } finally {
            closeStreams(partitionName, "commit", in, null);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/get/{base64PartitionName}/{consistency}/{checkLeader}")
    public Object get(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        InputStream inputStream) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }

        StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
        chunkedOutput.submit(chunkExecutors, partitionName, "get", new FilerInputStream(inputStream), 4096, (partitionName1, in, out) -> {
            client.get(partitionName1, Consistency.none, in, out);
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/getOffset/{base64PartitionName}/{consistency}/{checkLeader}")
    public Object getOffset(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        InputStream inputStream) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }

        StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
        chunkedOutput.submit(chunkExecutors, partitionName, "getOffset", new FilerInputStream(inputStream), 4096, (partitionName1, in, out) -> {
            client.getOffset(partitionName1, Consistency.none, in, out);
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/scan/{base64PartitionName}/{consistency}/{checkLeader}/{hydrateValues}")
    public Object scan(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        @PathParam("hydrateValues") boolean hydrateValues,
        InputStream inputStream) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }


        StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        List<ScanRange> ranges = Lists.newArrayList();
        FilerInputStream in = new FilerInputStream(inputStream);
        try {
            byte[] intLongBuffer = new byte[8];
            while (UIO.readByte(in, "eos") == (byte) 1) {
                byte[] fromPrefix = UIO.readByteArray(in, "fromPrefix", intLongBuffer);
                byte[] fromKey = UIO.readByteArray(in, "fromKey", intLongBuffer);
                byte[] toPrefix = UIO.readByteArray(in, "toPrefix", intLongBuffer);
                byte[] toKey = UIO.readByteArray(in, "toKey", intLongBuffer);

                byte[] from = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
                byte[] to = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
                if (from != null && to != null && KeyUtil.compare(from, to) > 0) {
                    return Response.status(Status.BAD_REQUEST).entity("Invalid range").build();
                }
                ranges.add(new ScanRange(fromPrefix, fromKey, toPrefix, toKey));
            }
        } catch (Exception e) {
            LOG.error("Failed to get ranges for stream scan", e);
            return Response.serverError().build();
        } finally {
            closeStreams(partitionName, "scan", in, null);
        }

        LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
        chunkedOutput.submit(chunkExecutors, partitionName, "scan", null, 4096, (partitionName1, in1, out) -> {
            client.scan(partitionName1, ranges, out, hydrateValues);
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/scanCompressed/{base64PartitionName}/{consistency}/{checkLeader}/{hydrateValues}")
    public Object scanCompressed(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        @PathParam("hydrateValues") boolean hydrateValues,
        InputStream inputStream) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }

        StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        List<ScanRange> ranges = Lists.newArrayList();
        FilerInputStream in = new FilerInputStream(inputStream);
        try {
            byte[] intLongBuffer = new byte[8];
            while (UIO.readByte(in, "eos") == (byte) 1) {
                byte[] fromPrefix = UIO.readByteArray(in, "fromPrefix", intLongBuffer);
                byte[] fromKey = UIO.readByteArray(in, "fromKey", intLongBuffer);
                byte[] toPrefix = UIO.readByteArray(in, "toPrefix", intLongBuffer);
                byte[] toKey = UIO.readByteArray(in, "toKey", intLongBuffer);

                byte[] from = fromKey != null ? WALKey.compose(fromPrefix, fromKey) : null;
                byte[] to = toKey != null ? WALKey.compose(toPrefix, toKey) : null;
                if (from != null && to != null && KeyUtil.compare(from, to) > 0) {
                    return Response.status(Status.BAD_REQUEST).entity("Invalid range").build();
                }
                ranges.add(new ScanRange(fromPrefix, fromKey, toPrefix, toKey));
            }
        } catch (Exception e) {
            LOG.error("Failed to get ranges for compressed stream scan", e);
            return Response.serverError().build();
        } finally {
            closeStreams(partitionName, "scanCompressed", in, null);
        }

        try {
            PartitionName effectivelyFinalPartitionName = partitionName;
            StreamingOutput stream = os -> {
                os.flush();
                SnappyOutputStream sos = new SnappyOutputStream(os);
                FilerOutputStream fos = new FilerOutputStream(new BufferedOutputStream(sos, 8192));
                try {
                    client.scan(effectivelyFinalPartitionName, ranges, fos, hydrateValues);
                } catch (Exception x) {
                    LOG.warn("Failed during compressed stream scan", x);
                } finally {
                    fos.close();
                }
            };
            return Response.ok(stream).build();
        } catch (Exception e) {
            LOG.error("Failed to compressed stream scan", e);
            return Response.serverError().build();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/takeFromTransactionId/{base64PartitionName}/{limit}")
    public Object takeFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("limit") int limit,
        InputStream inputStream) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }

        LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
        chunkedOutput.submit(chunkExecutors, partitionName, "takeFromTransactionId", new FilerInputStream(inputStream), 4096, (partitionName1, in, out) -> {
            client.takeFromTransactionId(partitionName1, limit, in, out);
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/takePrefixFromTransactionId/{base64PartitionName}/{limit}")
    public Object takePrefixFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("limit") int limit,
        InputStream inputStream) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }

        LatchChunkedOutput chunkedOutput = new LatchChunkedOutput(10_000);
        chunkedOutput.submit(chunkExecutors, partitionName, "takePrefixFromTransactionId", new FilerInputStream(inputStream), 4096,
            (partitionName1, in, out) -> {
                client.takePrefixFromTransactionId(partitionName1, limit, in, out);
            });
        return chunkedOutput;
    }

    private void closeStreams(PartitionName partitionName, String context, ICloseable in, ICloseable out) {
        if (in != null) {
            try {
                in.close();
            } catch (Exception x) {
                LOG.error("Failed to close input stream for {} {}", new Object[] { partitionName, context }, x);
            }
        }
        if (out != null) {
            try {
                out.close();
            } catch (Exception x) {
                LOG.error("Failed to close output stream for {} {}", new Object[] { partitionName, context }, x);
            }
        }
    }

    private Response stateMessageCauseToResponse(StateMessageCause stateMessageCause) {
        if (stateMessageCause != null && stateMessageCause.state != null) {
            LOG.warn("{}", stateMessageCause);
            switch (stateMessageCause.state) {
                case properties_not_present:
                    return ResponseHelper.INSTANCE.errorResponse(Status.NOT_FOUND, stateMessageCause.message, stateMessageCause.cause);
                case not_a_ring_member:
                    return ResponseHelper.INSTANCE.errorResponse(Status.SERVICE_UNAVAILABLE, stateMessageCause.message, stateMessageCause.cause);
                case failed_to_come_online:
                    return ResponseHelper.INSTANCE.errorResponse(Status.SERVICE_UNAVAILABLE, stateMessageCause.message, stateMessageCause.cause);
                case lacks_leader:
                    return ResponseHelper.INSTANCE.errorResponse(Status.SERVICE_UNAVAILABLE, stateMessageCause.message, stateMessageCause.cause);
                case not_the_leader:
                    return ResponseHelper.INSTANCE.errorResponse(Status.CONFLICT, stateMessageCause.message, stateMessageCause.cause);
                case error:
                    return ResponseHelper.INSTANCE.errorResponse(Status.INTERNAL_SERVER_ERROR, stateMessageCause.message, stateMessageCause.cause);
                default:
                    break;
            }
        }
        return null;
    }

    @GET
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/getApproximateCount/{base64PartitionName}/{consistency}/{checkLeader}")
    public Object getApproximateCount(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader) {

        PartitionName partitionName = null;
        try {
            partitionName = amzaInterner.internPartitionNameBase64(base64PartitionName);
        } catch (Exception x) {
            LOG.error("Failure while getting partitionName {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }

        StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        try {
            return Response.ok().entity(String.valueOf(client.approximateCount(partitionName))).build();
        } catch (Exception x) {
            LOG.error("Failure while getting approximate count for {}", new Object[] { partitionName }, x);
            return Response.serverError().build();
        }
    }
}
