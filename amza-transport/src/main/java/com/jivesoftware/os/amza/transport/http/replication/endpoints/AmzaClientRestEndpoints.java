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
import org.apache.http.HttpStatus;
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

    private Response failOnNotTheLeader(PartitionName partitionName, Consistency consistency) {
        if (consistency.requiresLeader()) {
            try {
                RingMember leader = partitionProvider.awaitLeader(partitionName, 0);
                if (leader == null) {
                    return Response.status(HttpStatus.SC_SERVICE_UNAVAILABLE).build();
                }
                if (!leader.equals(ringReader.getRingMember())) {
                    return Response.status(HttpStatus.SC_CONFLICT).build();
                }
            } catch (Exception x) {
                Object[] vals = new Object[] { partitionName, consistency };
                LOG.warn("Failed while determining leader {} at {}. ", vals, x);
                return ResponseHelper.INSTANCE.errorResponse("Failed while determining leader: " + Arrays.toString(vals), x);
            }
        }
        return null;
    }

    @POST
    //@Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/ring/{base64PartitionName}/{waitForLeaderElection}")
    public ChunkedOutput<byte[]> ring(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("waitForLeaderElection") long waitForLeaderElection) {

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
                RingMember leader = partitionName.isSystemPartition() ? null : partitionProvider.awaitLeader(partitionName, waitForLeaderElection);
                NavigableMap<RingMember, RingHost> ring = ringReader.getRing(partitionName.getRingName());

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
            Response response = checkLeader ? failOnNotTheLeader(partitionName, consistency) : null;
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
                        -1)) {
                        return false;
                    }
                }
                return true;
            }, timeoutInMillis);

            return Response.ok("success").build();
        } catch (Exception x) {
            Object[] vals = new Object[] { base64PartitionName, consistencyName };
            LOG.warn("Failed to commit to {} at {}. ", vals, x);
            return ResponseHelper.INSTANCE.errorResponse("Failed to commit: " + Arrays.toString(vals), x);
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
        Response response = checkLeader ? failOnNotTheLeader(partitionName, consistency) : null;
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
                        if (tombstoned) {
                            UIO.writeByteArray(cf, null, "value");
                            UIO.writeLong(cf, -1L, "timestamp");
                            UIO.writeBoolean(cf, true, "tombstoned");
                            UIO.writeLong(cf, -1L, "version");
                        } else {
                            UIO.writeByteArray(cf, value, "value");
                            UIO.writeLong(cf, timestamp, "timestamp");
                            UIO.writeBoolean(cf, false, "tombstoned");
                            UIO.writeLong(cf, version, "version");
                        }
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
    @Path("/scan/{base64PartitionName}/{consistency}/{checkLeader}")
    public Object scan(@PathParam("base64PartitionName") String base64PartitionName,
        @PathParam("consistency") String consistencyName,
        @PathParam("checkLeader") boolean checkLeader,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        Consistency consistency = Consistency.valueOf(consistencyName);
        FilerInputStream fis = new FilerInputStream(inputStream);
        Response response = checkLeader ? failOnNotTheLeader(partitionName, consistency) : null;
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
                partition.scan(fromPrefix, fromKey, toPrefix, toKey,
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
    @Path("/takeFromTransactionId/{base64PartitionName}")
    public Object takeFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        FilerInputStream fis = new FilerInputStream(inputStream);

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                long transactionId = UIO.readLong(fis, "transactionId");

                Partition partition = partitionProvider.getPartition(partitionName);
                take(chunkedOutput, partition, false, null, transactionId);
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
    @Path("/takePrefixFromTransactionId/{base64PartitionName}")
    public Object takePrefixFromTransactionId(@PathParam("base64PartitionName") String base64PartitionName,
        InputStream inputStream) {

        PartitionName partitionName = PartitionName.fromBase64(base64PartitionName);
        FilerInputStream fis = new FilerInputStream(inputStream);

        ChunkedOutput<byte[]> chunkedOutput = new ChunkedOutput<>(byte[].class);
        chunkExecutors.submit(() -> {
            try {
                Partition partition = partitionProvider.getPartition(partitionName);
                byte[] prefix = UIO.readByteArray(fis, "prefix");
                long txId = UIO.readLong(fis, "txId");
                take(chunkedOutput, partition, true, prefix, txId);
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

    private void take(ChunkedOutput<byte[]> chunkedOutput,
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
            takeResult = partition.takePrefixFromTransactionId(prefix, txId, streamHighwater, stream);
        } else {
            takeResult = partition.takeFromTransactionId(txId, streamHighwater, stream);
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
