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

import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.shared.Partition;
import com.jivesoftware.os.amza.shared.PartitionProvider;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.ring.AmzaRingReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.ResponseHelper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final AmzaRingReader ringReader;
    private final PartitionProvider partitionProvider;
    private final ExecutorService chunkExecutors = Executors.newCachedThreadPool(); // TODO config!!!

    public AmzaClientRestEndpoints(@Context AmzaRingReader ringReader,
        @Context PartitionProvider partitionProvider) {
        this.ringReader = ringReader;
        this.partitionProvider = partitionProvider;
    }

    private Response failOnNotTheLeader(PartitionName partitionName, Consistency consistency, FilerInputStream fis) {
        if (consistency.requiresLeader()) {
            try {
                RingMember expectedLeader = RingMember.fromBytes(UIO.readByteArray(fis, "leader"));
                if (expectedLeader != null) {
                    RingMember leader = ringReader.getLeader(partitionName.getRingName(), 0);
                    if (leader == null) {
                        return Response.status(503).build();
                    }
                    if (!expectedLeader.equals(leader)) {
                        return Response.status(409).build();
                    }
                } else {
                    // Some one failed to interacting with to leader..
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
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ring/{base64RingName}/{waitForLeaderElection}")
    public ChunkedOutput<byte[]> ring(@PathParam("base64RingName") String base64RingName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                byte[] ringName = BaseEncoding.base64Url().decode(base64RingName);
                RingMember leader = ringReader.getLeader(ringName, waitForLeaderElection);
                NavigableMap<RingMember, RingHost> ring = ringReader.getRing(ringName);

                ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                Set<Map.Entry<RingMember, RingHost>> memebers = ring.entrySet();
                UIO.writeInt(cf, memebers.size(), "ringSize");
                for (Map.Entry<RingMember, RingHost> r : memebers) {
                    UIO.writeByteArray(cf, r.getKey().toBytes(), "ringMember");
                    UIO.writeByteArray(cf, r.getValue().toBytes(), "ringHost");
                    boolean isLeader = leader != null && Arrays.equals(r.getKey().toBytes(), leader.toBytes());
                    UIO.writeBoolean(cf, isLeader, "leader");
                }
                cf.flush(true);

            } catch (Exception x) {
                LOG.warn("Failed to stream gets", x);
            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close get stream", x);
                }
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/commit/{base64PartitionName}/{consistency}")
    public Response commit(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        InputStream inputStream) {

        try {

            PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
            Consistency consistency = Consistency.valueOf(consistencyName);
            FilerInputStream fis = new FilerInputStream(inputStream);
            Response response = failOnNotTheLeader(partitionName, consistency, fis);
            if (response != null) {
                return response;
            }

            Partition partition = partitionProvider.getPartition(partitionName);
            byte[] prefix = UIO.readByteArray(fis, "prefix");
            long timeoutInMillis = UIO.readLong(fis, "timeoutInMillis");

            partition.commit(consistency, prefix, (highwaters, txKeyValueStream) -> {
                while (!UIO.readBoolean(fis, "eos")) {
                    if (!txKeyValueStream.row(UIO.readLong(fis, "rowTxId"),
                        UIO.readByteArray(fis, "key"),
                        UIO.readByteArray(fis, "value"),
                        UIO.readLong(fis, "valueTimestamp"),
                        UIO.readBoolean(fis, "valueTombstoned"),
                        UIO.readLong(fis, "valueVersion"))) {
                        return false;
                    }
                }
                return true;
            }, timeoutInMillis);

            return Response.ok("success").build();
        } catch (Exception x) {
            Object[] vals = new Object[]{base64PartitionName, consistencyName};
            LOG.warn("Failed to commit to {} at {}. ", vals, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to commit: " + Arrays.toString(vals), x);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/get/{base64PartitionName}/{consistency}")
    public Object get(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        Response response = failOnNotTheLeader(partitionName, consistency, fis);
        if (response != null) {
            return response;
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {

                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] prefix = UIO.readByteArray(fis, "prefix");

                ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                partition.get(consistency, prefix,
                    (keyStream) -> {
                        while (!UIO.readBoolean(fis, "eos")) {
                            if (!keyStream.stream(UIO.readByteArray(fis, "key"))) {
                                return false;
                            }
                        }
                        return true;
                    },
                    (prefix1, key, value, timestamp, tombstoned, version) -> {
                        UIO.writeBoolean(cf, false, "eos");
                        UIO.writeByteArray(cf, prefix1, "prefix");
                        UIO.writeByteArray(cf, key, "key");
                        UIO.writeByteArray(cf, value, "value");
                        UIO.writeLong(cf, timestamp, "timestamp");
                        UIO.writeBoolean(cf, tombstoned, "tombstoned");
                        UIO.writeLong(cf, version, "version");
                        return true;
                    });

                UIO.writeBoolean(cf, true, "eos");
                cf.flush(true);

            } catch (Exception x) {
                LOG.warn("Failed to stream gets", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close get stream", x);
                }
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/scan/{base64PartitionName}/{consistency}")
    public Object scan(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        Response response = failOnNotTheLeader(partitionName, consistency, fis);
        if (response != null) {
            return response;
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] fromPrefix = UIO.readByteArray(fis, "fromPrefix");
                byte[] fromKey = UIO.readByteArray(fis, "fromKey");
                byte[] toPrefix = UIO.readByteArray(fis, "toPrefix");
                byte[] toKey = UIO.readByteArray(fis, "toKey");

                ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
                partition.scan(consistency, fromPrefix, fromKey, toPrefix, toKey,
                    (prefix, key, value, timestamp, version) -> {
                        UIO.writeBoolean(cf, false, "eos");
                        UIO.writeByteArray(cf, prefix, "prefix");
                        UIO.writeByteArray(cf, key, "key");
                        UIO.writeByteArray(cf, value, "value");
                        UIO.writeLong(cf, timestamp, "timestampId");
                        UIO.writeLong(cf, version, "version");
                        return true;
                    });

                UIO.writeBoolean(cf, true, "eos");
                cf.flush(true);

            } catch (Exception x) {
                LOG.warn("Failed to stream scan", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close scan stream", x);
                }
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/takeFromTransactionId/{base64PartitionName}/{consistency}")
    public Object takeFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        Response response = failOnNotTheLeader(partitionName, consistency, fis);
        if (response != null) {
            return response;
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                long transactionId = UIO.readLong(fis, "transactionId");

                Partition partition = partitionProvider.getPartition(partitionName);
                take(consistency, chunkedOutput, partition, false, null, transactionId);
            } catch (Exception x) {
                LOG.warn("Failed to stream takeFromTransactionId", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close takeFromTransactionId stream", x);
                }
            }
        });
        return chunkedOutput;
    }

    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/takePrefixFromTransactionId/{base64PartitionName}/{consistency}")
    public Object takePrefixFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        Response response = failOnNotTheLeader(partitionName, consistency, fis);
        if (response != null) {
            return response;
        }

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] prefix = UIO.readByteArray(fis, "prefix");
                long txId = UIO.readLong(fis, "txId");
                take(consistency, chunkedOutput, partition, true, prefix, txId);
            } catch (Exception x) {
                LOG.warn("Failed to stream takePrefixFromTransactionId", x);

            } finally {
                try {
                    chunkedOutput.close();
                } catch (IOException x) {
                    LOG.warn("Failed to close takePrefixFromTransactionId stream", x);
                }
            }
        });
        return chunkedOutput;
    }

    private void take(Consistency consistency,
        ChunkedOutput<byte[]> chunkedOutput,
        Partition partition,
        boolean usePrefix,
        byte[] prefix,
        long txId) throws Exception {

        ChunkedOutputFiler cf = new ChunkedOutputFiler(new HeapFiler(new byte[4096]), chunkedOutput); // TODO config ?? or caller
        RingMember ringMember = ringReader.getRingMember();
        UIO.writeByteArray(cf, ringMember.toBytes(), "ringMember");
        Highwaters streamHighwater = (highwater) -> {
            UIO.writeBoolean(cf, false, "eos");
            UIO.writeByte(cf, RowType.highwater.toByte(), "type");
            writeHighwaters(cf, highwater);
        };
        TxKeyValueStream stream = (rowTxId, prefix1, key, value, timestamp, tombstoned, version) -> {
            UIO.writeBoolean(cf, false, "eos");
            UIO.writeByte(cf, RowType.primary.toByte(), "type");
            UIO.writeLong(cf, rowTxId, "rowTxId");
            UIO.writeByteArray(cf, prefix1, "prefix");
            UIO.writeByteArray(cf, key, "key");
            UIO.writeByteArray(cf, value, "value");
            UIO.writeLong(cf, timestamp, "timestamp");
            UIO.writeBoolean(cf, tombstoned, "tombstoned");
            UIO.writeLong(cf, version, "version");
            return true;
        };
        TakeResult takeResult;
        if (usePrefix) {
            takeResult = partition.takePrefixFromTransactionId(consistency, prefix, txId, streamHighwater, stream);
        } else {
            takeResult = partition.takeFromTransactionId(consistency, txId, streamHighwater, stream);
        }
        UIO.writeBoolean(cf, true, "eos");

        UIO.writeByteArray(cf, takeResult.tookFrom.toBytes(), "ringMember");
        UIO.writeLong(cf, takeResult.lastTxId, "lastTxId");
        writeHighwaters(cf, takeResult.tookToEnd);
        UIO.writeBoolean(cf, true, "eos");
        cf.flush(true);
    }

    private void writeHighwaters(ChunkedOutputFiler cf, WALHighwater highwater) throws IOException {
        UIO.writeInt(cf, highwater.ringMemberHighwater.size(), "length");
        for (WALHighwater.RingMemberHighwater ringMemberHighwater : highwater.ringMemberHighwater) {
            UIO.writeByteArray(cf, ringMemberHighwater.ringMember.toBytes(), "ringMember");
            UIO.writeLong(cf, ringMemberHighwater.transactionId, "txId");
        }
    }

}
