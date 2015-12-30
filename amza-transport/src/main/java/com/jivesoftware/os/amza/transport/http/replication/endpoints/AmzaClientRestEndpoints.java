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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.ICloseable;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.glassfish.jersey.server.ChunkedOutput;

@Singleton
@Path("/amza/v1")
public class AmzaClientRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRestClient client;
    private final ExecutorService chunkExecutors = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("stream-chunks-%d").build());

    public AmzaClientRestEndpoints(@Context AmzaRestClient client) {
        this.client = client;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/configPartition/{base64PartitionName}/{ringSize}")
    public Object configPartition(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("ringSize") int ringSize,
        PartitionProperties partitionProperties) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        try {
            RingTopology ringTopology = client.configPartition(partitionName, partitionProperties, ringSize);
            ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
            chunkExecutors.submit(() -> {
                ChunkedOutputFiler out = null;
                try {
                    out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                    client.configPartition(ringTopology, out);
                    out.flush(true);
                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    closeStreams("configPartition", null, out);
                }
            });
            return chunkedOutput;
        } catch (Exception e) {
            LOG.error("Failed while attempting to configPartition:{} {} {}", new Object[]{partitionName, partitionProperties, ringSize}, e);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed while attempting to configPartition.", e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/ensurePartition/{base64PartitionName}/{waitForLeaderElection}")
    public Object ensurePartition(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        try {
            client.ensurePartition(partitionName, waitForLeaderElection);
            return Response.ok().build();
        } catch (TimeoutException e) {
            LOG.error("No leader elected within timeout:{} {} millis", new Object[]{partitionName, waitForLeaderElection}, e);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.SERVICE_UNAVAILABLE, "No leader elected within timeout.", e);
        } catch (Exception e) {
            LOG.error("Failed while attempting to ensurePartition:{}", new Object[]{partitionName}, e);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed while attempting to ensurePartition.", e);
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ring/{base64PartitionName}")
    public Object ring(@PathParam("base64PartitionName") String base64PartitionName) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        try {
            AmzaClientService.RingLeader ringLeader = client.ring(partitionName);
            ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
            chunkExecutors.submit(() -> {
                ChunkedOutputFiler out = null;
                try {
                    out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                    client.ring(ringLeader, out);
                    out.flush(true);
                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    closeStreams("commit", null, out);
                }
            });
            return chunkedOutput;
        } catch (Exception e) {
            LOG.error("Failed while attempting to get ring:{}", new Object[]{partitionName}, e);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed while attempting to ensurePartition.", e);
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ringLeader/{base64PartitionName}/{waitForLeaderElection}")
    public Object ringLeader(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        try {
            AmzaClientService.RingLeader ringLeader = client.ringLeader(partitionName, waitForLeaderElection);
            ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
            chunkExecutors.submit(() -> {
                ChunkedOutputFiler out = null;
                try {
                    out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                    client.ring(ringLeader, out);
                    out.flush(true);
                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    closeStreams("commit", null, out);
                }
            });
            return chunkedOutput;
        } catch (TimeoutException e) {
            LOG.error("No leader elected within timeout:{} {} millis", new Object[]{partitionName, waitForLeaderElection}, e);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.SERVICE_UNAVAILABLE, "No leader elected within timeout.", e);
        } catch (Exception e) {
            LOG.error("Failed while attempting to get ring:{}", new Object[]{partitionName}, e);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Failed while attempting to ensurePartition.", e);
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

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        FilerInputStream in = null;
        try {
            in = new FilerInputStream(inputStream);
            AmzaClientService.StateMessageCause stateMessageCause = client.commit(partitionName,
                Consistency.valueOf(consistencyName),
                checkLeader, 10_000, in);
            if (stateMessageCause != null) {
                return stateMessageCauseToResponse(stateMessageCause);
            }
            return Response.ok("success").build();

        } catch (DeltaOverCapacityException x) {
            LOG.warn("Delta over capacity for {} {}", base64PartitionName, x);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.SERVICE_UNAVAILABLE, "Delta over capacity.");
        } catch (FailedToAchieveQuorumException x) {
            LOG.warn("FailedToAchieveQuorumException for {} {}", base64PartitionName, x);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.ACCEPTED, "Failed to achieve quorum exception.");
        } catch (Exception x) {
            Object[] vals = new Object[]{partitionName, consistencyName};
            LOG.warn("Failed to commit to {} at {}.", vals, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to commit: " + Arrays.toString(vals), x);
        } finally {
            closeStreams("commit", in, null);
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

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        AmzaClientService.StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            FilerInputStream in = null;
            ChunkedOutputFiler out = null;
            try {
                in = new FilerInputStream(inputStream);
                out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                client.get(partitionName, Consistency.none, in, out);
                out.flush(true);
            } catch (Exception x) {
                LOG.warn("Failed to stream gets", x);
            } finally {
                closeStreams("get", in, out);
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/scan/{base64PartitionName}/{consistency}/{checkLeader}")
    public Object scan(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        AmzaClientService.StateMessageCause stateMessageCause = client.status(partitionName,
            Consistency.valueOf(consistencyName),
            checkLeader,
            10_000);
        if (stateMessageCause != null) {
            return stateMessageCauseToResponse(stateMessageCause);
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            FilerInputStream in = null;
            ChunkedOutputFiler out = null;
            try {
                in = new FilerInputStream(inputStream);
                out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                client.scan(partitionName, in, out);
                out.flush(true);

            } catch (Exception x) {
                LOG.warn("Failed to stream scan", x);
            } finally {
                closeStreams("scan", in, out);
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/takeFromTransactionId/{base64PartitionName}")
    public Object takeFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        InputStream inputStream) {

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            FilerInputStream in = null;
            ChunkedOutputFiler out = null;
            try {
                in = new FilerInputStream(inputStream);
                out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                client.takeFromTransactionId(PartitionName.fromBase64(base64PartitionName), in, out);
                out.flush(true);
            } catch (Exception x) {
                LOG.warn("Failed to stream takeFromTransactionId", x);
            } finally {
                closeStreams("takeFromTransactionId", in, out);
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/takePrefixFromTransactionId/{base64PartitionName}")
    public Object takePrefixFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        InputStream inputStream) {

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {

            FilerInputStream in = null;
            ChunkedOutputFiler out = null;
            try {
                in = new FilerInputStream(inputStream);
                out = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                client.takePrefixFromTransactionId(PartitionName.fromBase64(base64PartitionName), in, out);
                out.flush(true);
            } catch (Exception x) {
                LOG.warn("Failed to stream takePrefixFromTransactionId", x);
            } finally {
                closeStreams("takePrefixFromTransactionId", in, out);
            }
        });
        return chunkedOutput;
    }

    private void closeStreams(String context, ICloseable in, ICloseable out) {
        if (in != null) {
            try {
                in.close();
            } catch (Exception x) {
                LOG.warn("Failed to close input stream for {} {}", context, x);
            }
        }
        if (out != null) {
            try {
                out.close();
            } catch (Exception x) {
                LOG.warn("Failed to close output stream for {} {}", context, x);
            }
        }
    }

    private Response stateMessageCauseToResponse(AmzaClientService.StateMessageCause stateMessageCause) {
        if (stateMessageCause != null && stateMessageCause.state != null) {
            LOG.warn("{}", stateMessageCause);
            switch (stateMessageCause.state) {
                case failed_to_come_online:
                    return ResponseHelper.INSTANCE.errorResponse(Response.Status.SERVICE_UNAVAILABLE, stateMessageCause.message, stateMessageCause.cause);
                case lacks_leader:
                    return ResponseHelper.INSTANCE.errorResponse(Response.Status.SERVICE_UNAVAILABLE, stateMessageCause.message, stateMessageCause.cause);
                case not_the_leader:
                    return ResponseHelper.INSTANCE.errorResponse(Response.Status.CONFLICT, stateMessageCause.message, stateMessageCause.cause);
                case error:
                    return ResponseHelper.INSTANCE.errorResponse(Response.Status.INTERNAL_SERVER_ERROR, stateMessageCause.message, stateMessageCause.cause);
                default:
                    break;
            }
        }
        return null;
    }

}
