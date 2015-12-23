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

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.DeltaOverCapacityException;
import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.Partition;
import com.jivesoftware.os.amza.shared.PartitionProvider;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.amza.shared.ring.AmzaRingWriter;
import com.jivesoftware.os.amza.shared.ring.RingTopology;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.IOException;
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
import org.apache.http.HttpStatus;
import org.glassfish.jersey.server.ChunkedOutput;

@Singleton
@Path("/amza/v1")
public class AmzaClientRestEndpoints {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private final AmzaRingReader ringReader;
    private final AmzaRingWriter ringWriter;
    private final PartitionProvider partitionProvider;
    private final ExecutorService chunkExecutors = Executors.newCachedThreadPool(); // TODO config!!!

    public AmzaClientRestEndpoints(@Context AmzaRingReader ringReader,
        @Context AmzaRingWriter ringWriter,
        @Context PartitionProvider partitionProvider) {
        this.ringReader = ringReader;
        this.ringWriter = ringWriter;
        this.partitionProvider = partitionProvider;
    }

    private Response checkForReadyState(PartitionName partitionName, Consistency consistency, boolean checkLeader) {
        try {
            partitionProvider.awaitOnline(partitionName, 10_000L); //TODO config
        } catch (Exception e) {
            return Response.status(HttpStatus.SC_SERVICE_UNAVAILABLE).build();
        }
        if (checkLeader && consistency.requiresLeader()) {
            try {
                RingMember leader = partitionProvider.awaitLeader(partitionName, 0);
                if (leader == null) {
                    return Response.status(HttpStatus.SC_SERVICE_UNAVAILABLE).build();
                }
                if (!leader.equals(ringReader.getRingMember())) {
                    return Response.status(HttpStatus.SC_CONFLICT).build();
                }
            } catch (Exception x) {
                Object[] vals = new Object[]{partitionName, consistency};
                LOG.warn("Failed while determining leader {} at {}. ", vals, x);
                return ResponseHelper.INSTANCE.errorResponse("Failed while determining leader: " + Arrays.toString(vals), x);
            }
        }
        return null;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/configPartition/{base64PartitionName}/{ringSize}")
    public Object configPartition(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("ringSize") int ringSize,
        PartitionProperties partitionProperties) {

        try {
            PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
            byte[] ringNameBytes = partitionName.getRingName();
            ringWriter.ensureSubRing(ringNameBytes, ringSize);
            partitionProvider.setPropertiesIfAbsent(partitionName, partitionProperties);
            RingTopology ring = ringReader.getRing(partitionName.getRingName());

            ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
            chunkExecutors.submit(() -> {
                try {
                    ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                    byte[] lengthBuffer = new byte[4];
                    UIO.writeInt(cf, ring.entries.size(), "ringSize", lengthBuffer);
                    for (RingMemberAndHost entry : ring.entries) {
                        UIO.writeByteArray(cf, entry.ringMember.toBytes(), "ringMember", lengthBuffer);
                        UIO.writeByteArray(cf, entry.ringHost.toBytes(), "ringHost", lengthBuffer);
                        UIO.writeByte(cf, (byte) 0, "leader");
                    }
                    cf.flush(true);

                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    try {
                        chunkedOutput.close();
                    } catch (IOException x) {
                        LOG.warn("Failed to close ring stream", x);
                    }
                }
            });
            return chunkedOutput;
        } catch (TimeoutException e) {
            return Response.status(HttpStatus.SC_SERVICE_UNAVAILABLE).build();
        } catch (Exception e) {
            return Response.serverError().build();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/ensurePartition/{base64PartitionName}/{waitForLeaderElection}")
    public Object ensurePartition(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        try {
            PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
            long start = System.currentTimeMillis();
            partitionProvider.awaitOnline(partitionName, waitForLeaderElection);
            waitForLeaderElection = Math.max(0, waitForLeaderElection - (System.currentTimeMillis() - start));
            partitionProvider.awaitLeader(partitionName, waitForLeaderElection);
            return Response.ok().build();
        } catch (TimeoutException e) {
            return Response.status(HttpStatus.SC_SERVICE_UNAVAILABLE).build();
        } catch (Exception e) {
            return Response.serverError().build();
        }
    }

    @POST
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ring/{base64PartitionName}/{waitForLeaderElection}")
    public Object ring(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        try {
            PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
            RingMember leader = partitionName.isSystemPartition() ? null : partitionProvider.awaitLeader(partitionName, waitForLeaderElection);
            RingTopology ring = ringReader.getRing(partitionName.getRingName());

            ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
            chunkExecutors.submit(() -> {
                try {
                    ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                    byte[] lengthBuffer = new byte[4];
                    UIO.writeInt(cf, ring.entries.size(), "ringSize", lengthBuffer);
                    for (RingMemberAndHost entry : ring.entries) {
                        UIO.writeByteArray(cf, entry.ringMember.toBytes(), "ringMember", lengthBuffer);
                        UIO.writeByteArray(cf, entry.ringHost.toBytes(), "ringHost", lengthBuffer);
                        boolean isLeader = leader != null && Arrays.equals(entry.ringMember.toBytes(), leader.toBytes());
                        UIO.writeByte(cf, isLeader ? (byte) 1 : (byte) 0, "leader");
                    }
                    cf.flush(true);

                } catch (Exception x) {
                    LOG.warn("Failed to stream ring", x);
                } finally {
                    try {
                        chunkedOutput.close();
                    } catch (IOException x) {
                        LOG.warn("Failed to close ring stream", x);
                    }
                }
            });
            return chunkedOutput;
        } catch (TimeoutException e) {
            return Response.status(HttpStatus.SC_SERVICE_UNAVAILABLE).build();
        } catch (Exception e) {
            return Response.serverError().build();
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

        try {

            PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
            Consistency consistency = Consistency.valueOf(consistencyName);
            FilerInputStream fis = new FilerInputStream(inputStream);
            Response response = checkForReadyState(partitionName, consistency, checkLeader);
            if (response != null) {
                return response;
            }

            Partition partition = partitionProvider.getPartition(partitionName);
            byte[] intLongBuffer = new byte[8];
            byte[] prefix = UIO.readByteArray(fis, "prefix", intLongBuffer);
            long timeoutInMillis = UIO.readLong(fis, "timeoutInMillis", intLongBuffer);

            partition.commit(consistency, prefix, (highwaters, txKeyValueStream) -> {
                while (!UIO.readBoolean(fis, "eos")) {
                    if (!txKeyValueStream.row(UIO.readLong(fis, "rowTxId", intLongBuffer),
                        UIO.readByteArray(fis, "key", intLongBuffer),
                        UIO.readByteArray(fis, "value", intLongBuffer),
                        UIO.readLong(fis, "valueTimestamp", intLongBuffer),
                        UIO.readBoolean(fis, "valueTombstoned"),
                        -1)) {
                        return false;
                    }
                }
                return true;
            }, timeoutInMillis);

            return Response.ok("success").build();
        } catch (DeltaOverCapacityException x) {
            LOG.info("Delta over capacity for {}", base64PartitionName);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.SERVICE_UNAVAILABLE, "Delta over capacity");
        } catch (FailedToAchieveQuorumException x) {
            LOG.info("FailedToAchieveQuorumException for {}", base64PartitionName);
            return ResponseHelper.INSTANCE.errorResponse(Response.Status.ACCEPTED, "Failed to achieve quorum exception");
        } catch (Exception x) {
            Object[] vals = new Object[]{base64PartitionName, consistencyName};
            LOG.warn("Failed to commit to {} at {}.", vals, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to commit: " + Arrays.toString(vals), x);
        } finally {
            try {
                inputStream.close();
            } catch (IOException x) {
                LOG.warn("Failed to close input stream", x);
            }
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
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        Response response = checkForReadyState(partitionName, consistency, checkLeader);
        if (response != null) {
            return response;
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        byte[] intLongBuffer = new byte[8];
        chunkExecutors.submit(() -> {
            try {

                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] prefix = UIO.readByteArray(fis, "prefix", intLongBuffer);

                ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                partition.get(consistency, prefix,
                    (keyStream) -> {
                        while (!UIO.readBoolean(fis, "eos")) {
                            if (!keyStream.stream(UIO.readByteArray(fis, "key", intLongBuffer))) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (prefix1, key, value, timestamp, tombstoned, version) -> {
                        UIO.writeByte(cf, (byte) 0, "eos");
                        UIO.writeByteArray(cf, prefix1, "prefix", intLongBuffer);
                        UIO.writeByteArray(cf, key, "key", intLongBuffer);
                        if (tombstoned) {
                            UIO.writeByteArray(cf, null, "value", intLongBuffer);
                            UIO.writeLong(cf, -1L, "timestamp");
                            UIO.writeByte(cf, (byte) 1, "tombstoned");
                            UIO.writeLong(cf, -1L, "version");
                        } else {
                            UIO.writeByteArray(cf, value, "value", intLongBuffer);
                            UIO.writeLong(cf, timestamp, "timestamp");
                            UIO.writeByte(cf, (byte) 0, "tombstoned");
                            UIO.writeLong(cf, version, "version");
                        }
                        return true;
                    });

                UIO.writeByte(cf, (byte) 1, "eos");
                cf.flush(true);

            } catch (Exception x) {
                LOG.warn("Failed to stream gets", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close get stream", x);
                }
                try {
                    inputStream.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close input stream", x);
                }
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
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);

        Response response = checkForReadyState(partitionName, consistency, checkLeader);
        if (response != null) {
            return response;
        }
        byte[] intLongBuffer = new byte[8];
        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] fromPrefix = UIO.readByteArray(fis, "fromPrefix", intLongBuffer);
                byte[] fromKey = UIO.readByteArray(fis, "fromKey", intLongBuffer);
                byte[] toPrefix = UIO.readByteArray(fis, "toPrefix", intLongBuffer);
                byte[] toKey = UIO.readByteArray(fis, "toKey", intLongBuffer);

                ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                partition.scan(fromPrefix, fromKey, toPrefix, toKey,
                    (prefix, key, value, timestamp, version) -> {
                        UIO.writeByte(cf, (byte) 0, "eos");
                        UIO.writeByteArray(cf, prefix, "prefix", intLongBuffer);
                        UIO.writeByteArray(cf, key, "key", intLongBuffer);
                        UIO.writeByteArray(cf, value, "value", intLongBuffer);
                        UIO.writeLong(cf, timestamp, "timestampId");
                        UIO.writeLong(cf, version, "version");
                        return true;
                    });

                UIO.writeByte(cf, (byte) 1, "eos");
                cf.flush(true);

            } catch (Exception x) {
                LOG.warn("Failed to stream scan", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close scan stream", x);
                }
                try {
                    inputStream.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close input stream", x);
                }
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

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        byte[] intLongBuffer = new byte[8];
        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                long transactionId = UIO.readLong(fis, "transactionId", intLongBuffer);

                Partition partition = partitionProvider.getPartition(partitionName);
                take(chunkedOutput, partition, false, null, transactionId, intLongBuffer);
            } catch (Exception x) {
                LOG.warn("Failed to stream takeFromTransactionId", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close takeFromTransactionId stream", x);
                }
                try {
                    inputStream.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close input stream", x);
                }
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

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        byte[] intLongBuffer = new byte[8];
        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] prefix = UIO.readByteArray(fis, "prefix", intLongBuffer);
                long txId = UIO.readLong(fis, "txId", intLongBuffer);
                take(chunkedOutput, partition, true, prefix, txId, intLongBuffer);
            } catch (Exception x) {
                LOG.warn("Failed to stream takePrefixFromTransactionId", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close takePrefixFromTransactionId stream", x);
                }
                try {
                    inputStream.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close input stream", x);
                }
            }
        });
        return chunkedOutput;
    }

    private void take(ChunkedOutput<byte[]> chunkedOutput,
        Partition partition,
        boolean usePrefix,
        byte[] prefix,
        long txId,
        byte[] lengthBuffer) throws Exception {

        ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
        RingMember ringMember = ringReader.getRingMember();
        UIO.writeByteArray(cf, ringMember.toBytes(), "ringMember", lengthBuffer);
        Highwaters streamHighwater = (highwater) -> {
            UIO.writeByte(cf, (byte) 0, "eos");
            UIO.writeByte(cf, RowType.highwater.toByte(), "type");
            writeHighwaters(cf, highwater, lengthBuffer);
        };
        TxKeyValueStream stream = (rowTxId, prefix1, key, value, timestamp, tombstoned, version) -> {
            UIO.writeByte(cf, (byte) 0, "eos");
            UIO.writeByte(cf, RowType.primary.toByte(), "type");
            UIO.writeLong(cf, rowTxId, "rowTxId");
            UIO.writeByteArray(cf, prefix1, "prefix", lengthBuffer);
            UIO.writeByteArray(cf, key, "key", lengthBuffer);
            UIO.writeByteArray(cf, value, "value", lengthBuffer);
            UIO.writeLong(cf, timestamp, "timestamp");
            UIO.writeByte(cf, tombstoned ? (byte) 1 : (byte) 0, "tombstoned");
            UIO.writeLong(cf, version, "version");
            return true;
        };
        TakeResult takeResult;
        if (usePrefix) {
            takeResult = partition.takePrefixFromTransactionId(prefix, txId, streamHighwater, stream);
        } else {
            takeResult = partition.takeFromTransactionId(txId, streamHighwater, stream);
        }
        UIO.writeByte(cf, (byte) 1, "eos");

        UIO.writeByteArray(cf, takeResult.tookFrom.toBytes(), "ringMember", lengthBuffer);
        UIO.writeLong(cf, takeResult.lastTxId, "lastTxId");
        writeHighwaters(cf, takeResult.tookToEnd, lengthBuffer);
        UIO.writeByte(cf, (byte) 1, "eos");
        cf.flush(true);
    }

    private void writeHighwaters(ChunkedOutputFiler cf, WALHighwater highwater, byte[] lengthBuffer) throws IOException {
        UIO.writeInt(cf, highwater.ringMemberHighwater.size(), "length", lengthBuffer);
        for (WALHighwater.RingMemberHighwater ringMemberHighwater : highwater.ringMemberHighwater) {
            UIO.writeByteArray(cf, ringMemberHighwater.ringMember.toBytes(), "ringMember", lengthBuffer);
            UIO.writeLong(cf, ringMemberHighwater.transactionId, "txId");
        }
    }

}
