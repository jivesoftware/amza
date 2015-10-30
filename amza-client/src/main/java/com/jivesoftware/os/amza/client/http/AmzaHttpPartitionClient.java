package com.jivesoftware.os.amza.client.http;

import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.FilerOutputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
import com.jivesoftware.os.amza.client.http.exceptions.LeaderElectionInProgressException;
import com.jivesoftware.os.amza.client.http.exceptions.NoLongerTheLeaderException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;

/**
 * @author jonathan.colt
 */
public class AmzaHttpPartitionClient implements PartitionClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String base64PartitionName;
    private final PartitionName partitionName;
    private final AmzaHttpClientCallRouter partitionCallRouter;
    private final long awaitLeaderElectionForNMillis;

    public AmzaHttpPartitionClient(PartitionName partitionName,
        AmzaHttpClientCallRouter partitionCallRouter,
        long awaitLeaderElectionForNMillis) throws IOException {

        this.base64PartitionName = partitionName.toBase64();
        this.partitionName = partitionName;
        this.partitionCallRouter = partitionCallRouter;
        this.awaitLeaderElectionForNMillis = awaitLeaderElectionForNMillis;
    }

    private void handleLeaderStatusCodes(Consistency consistency, int statusCode, Closeable closeable) {
        if (consistency.requiresLeader()) {
            if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close {}", closeable);
                }
                partitionCallRouter.invalidateRouting(partitionName);
                throw new LeaderElectionInProgressException(partitionName + " " + consistency);
            }
            if (statusCode == HttpStatus.SC_CONFLICT) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close {}", closeable);
                }
                partitionCallRouter.invalidateRouting(partitionName);
                throw new NoLongerTheLeaderException(partitionName + " " + consistency);
            }
        }
    }

    private static class CloseableHttpResponse implements Closeable {

        public final HttpResponse response;

        public CloseableHttpResponse(HttpResponse response) {
            this.response = response;
        }

        @Override
        public void close() throws Exception {
        }
    }

    private static class CloseableHttpStreamResponse implements Closeable {

        public final HttpStreamResponse response;

        public CloseableHttpStreamResponse(HttpStreamResponse response) {
            this.response = response;
        }

        @Override
        public void close() throws Exception {
            response.close();
        }
    }

    @Override
    public void commit(Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        byte[] lengthBuffer = new byte[4];

        partitionCallRouter.write(solutionLog.orElse(null), partitionName, consistency, "commit",
            (leader, ringMember, client) -> {
                boolean checkLeader = ringMember.equals(leader);
                HttpResponse got = client.postStreamableRequest("/amza/v1/commit/" + base64PartitionName + "/" + consistency.name() + "/" + checkLeader,
                    (out) -> {
                        try {

                            FilerOutputStream fos = new FilerOutputStream(out);
                            UIO.writeByteArray(fos, prefix, 0, prefix.length, "prefix", lengthBuffer);
                            UIO.writeLong(fos, abandonSolutionAfterNMillis, "timeoutInMillis");

                            updates.updates((rowTxId, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                                UIO.write(fos, new byte[]{0}, "eos");
                                UIO.writeLong(fos, rowTxId, "rowTxId");
                                UIO.writeByteArray(fos, key, 0, key.length, "key", lengthBuffer);
                                UIO.writeByteArray(fos, value, 0, value.length, "value", lengthBuffer);
                                UIO.writeLong(fos, valueTimestamp, "valueTimestamp");
                                UIO.write(fos, new byte[]{valueTombstoned ? (byte) 1 : (byte) 0}, "valueTombstoned");
                                // valueVersion is only ever generated on the servers.
                                return true;
                            });
                            UIO.write(fos, new byte[]{1}, "eos");
                        } catch (Exception x) {
                            throw new RuntimeException("Failed while streaming commitable.", x);
                        }
                    }, null);

                CloseableHttpResponse closeableHttpResponse = new CloseableHttpResponse(got);
                handleLeaderStatusCodes(consistency, got.getStatusCode(), closeableHttpResponse);
                return new PartitionResponse<>(closeableHttpResponse, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            },
            answer -> true,
            awaitLeaderElectionForNMillis,
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis);
    }

    @Override
    public boolean get(Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys,
        KeyValueTimestampStream valuesStream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        byte[] intLongBuffer = new byte[8];
        partitionCallRouter.read(solutionLog.orElse(null), partitionName, consistency, "get",
            (leader, ringMember, client) -> {
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/get/" + base64PartitionName + "/" + consistency.name() + "/" + ringMember.equals(leader),
                    (out) -> {
                        try {
                            FilerOutputStream fos = new FilerOutputStream(out);
                            UIO.writeByteArray(fos, prefix, 0, prefix.length, "prefix", intLongBuffer);
                            keys.consume((key) -> {
                                UIO.write(fos, new byte[]{0}, "eos");
                                UIO.writeByteArray(fos, key, 0, key.length, "key", intLongBuffer);
                                return true;
                            });
                            UIO.write(fos, new byte[]{1}, "eos");
                        } catch (Exception x) {
                            throw new RuntimeException("Failed while streaming keys.", x);
                        }
                    }, null);
                CloseableHttpStreamResponse closeableHttpStreamResponse = new CloseableHttpStreamResponse(got);
                handleLeaderStatusCodes(consistency, got.getStatusCode(), closeableHttpStreamResponse);
                return new PartitionResponse<>(closeableHttpStreamResponse, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            },
            (answers) -> {
                List<FilerInputStream> streams = Lists.transform(answers, input -> new FilerInputStream(input.getAnswer().response.getInputStream()));
                int eosed = 0;
                while (streams.size() > 0 && eosed == 0) {
                    byte[] latestPrefix = null;
                    byte[] latestKey = null;
                    byte[] latestValue = null;
                    long latestTimestamp = Long.MIN_VALUE;
                    long latestVersion = Long.MIN_VALUE;
                    for (FilerInputStream fis : streams) {
                        if (!UIO.readBoolean(fis, "eos")) {
                            byte[] p = UIO.readByteArray(fis, "prefix", intLongBuffer);
                            byte[] k = UIO.readByteArray(fis, "key", intLongBuffer);
                            byte[] v = UIO.readByteArray(fis, "value", intLongBuffer);
                            long t = UIO.readLong(fis, "timestamp", intLongBuffer);
                            boolean d = UIO.readBoolean(fis, "tombstone");
                            long z = UIO.readLong(fis, "version", intLongBuffer);

                            int c = CompareTimestampVersions.compare(t, z, latestTimestamp, latestVersion);
                            if (c > 0) {
                                latestPrefix = p;
                                latestKey = k;
                                latestValue = v;
                                latestTimestamp = t;
                                latestVersion = z;
                            }
                        } else {
                            eosed++;
                        }
                    }
                    if (eosed > 0 && eosed < answers.size()) {
                        throw new RuntimeException("Mismatched response lengths");
                    }
                    if (eosed == 0 && !valuesStream.stream(latestPrefix, latestKey, latestValue, latestTimestamp, latestVersion)) {
                        break;
                    }
                }
                return null;
            },
            awaitLeaderElectionForNMillis,
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis);
        return true;
    }

    @Override
    public boolean scan(Consistency consistency,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueTimestampStream stream,
        long additionalSolverAfterNMillis,
        long abandonLeaderSolutionAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        boolean merge;
        if (consistency == Consistency.leader_plus_one
            || consistency == Consistency.leader_quorum
            || consistency == Consistency.quorum
            || consistency == Consistency.write_one_read_all) {
            merge = true;
        } else {
            merge = false;
        }
        byte[] intLongBuffer = new byte[8];
        return partitionCallRouter.read(solutionLog.orElse(null), partitionName, consistency, "scan",
            (leader, ringMember, client) -> {
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/scan/" + base64PartitionName + "/" + consistency.name() + "/" + ringMember.equals(leader),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, fromPrefix, 0, fromPrefix.length, "fromPrefix", intLongBuffer);
                        UIO.writeByteArray(fos, fromKey, 0, fromKey.length, "fromKey", intLongBuffer);
                        UIO.writeByteArray(fos, toPrefix, 0, toPrefix.length, "toPrefix", intLongBuffer);
                        UIO.writeByteArray(fos, toKey, 0, toKey.length, "toKey", intLongBuffer);
                    }, null);

                return new PartitionResponse<>(new CloseableHttpStreamResponse(got), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            },
            (answers) -> {
                List<FilerInputStream> streams = Lists.transform(answers, input -> new FilerInputStream(input.getAnswer().response.getInputStream()));
                int size = streams.size();
                if (merge && size > 1) {
                    boolean[] eos = new boolean[size];
                    QuorumScan quorumScan = new QuorumScan(size);
                    int eosed = 0;
                    while (eosed < size) {
                        for (int i = 0; i < size; i++) {
                            if (quorumScan.used(i) && !eos[i]) {
                                FilerInputStream fis = streams.get(i);
                                eos[i] = UIO.readBoolean(fis, "eos");
                                if (!eos[i]) {
                                    quorumScan.fill(i, UIO.readByteArray(fis, "prefix", intLongBuffer),
                                        UIO.readByteArray(fis, "key", intLongBuffer),
                                        UIO.readByteArray(fis, "value", intLongBuffer),
                                        UIO.readLong(fis, "timestampId", intLongBuffer),
                                        UIO.readLong(fis, "version", intLongBuffer));
                                } else {
                                    eosed++;
                                }
                            }
                        }
                        int wi = quorumScan.findWinningIndex();
                        if (wi == -1 || !quorumScan.stream(wi, stream)) {
                            return false;
                        }
                    }
                    int wi;
                    while ((wi = quorumScan.findWinningIndex()) > -1) {
                        if (!quorumScan.stream(wi, stream)) {
                            return false;
                        }
                    }
                    LOG.info("Merged {}", answers.size());
                    return true;

                } else {
                    for (FilerInputStream fis : streams) {
                        while (!UIO.readBoolean(fis, "eos")) {
                            if (!stream.stream(UIO.readByteArray(fis, "prefix", intLongBuffer),
                                UIO.readByteArray(fis, "key", intLongBuffer),
                                UIO.readByteArray(fis, "value", intLongBuffer),
                                UIO.readLong(fis, "timestampId", intLongBuffer),
                                UIO.readLong(fis, "version", intLongBuffer))) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                throw new RuntimeException("Failed to scan.");
            },
            awaitLeaderElectionForNMillis,
            additionalSolverAfterNMillis,
            abandonLeaderSolutionAfterNMillis,
            abandonSolutionAfterNMillis);
    }

    @Override
    public TakeResult takeFromTransactionId(List<RingMember> membersInOrder,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {

        byte[] intLongBuffer = new byte[8];
        return partitionCallRouter.take(solutionLog.orElse(null), partitionName, membersInOrder, "takeFromTransactionId",
            (leader, ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/takeFromTransactionId/" + base64PartitionName,
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);

                return new PartitionResponse<>(new CloseableHttpStreamResponse(got), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            },
            (answers) -> {
                List<FilerInputStream> streams = Lists.transform(answers, input -> new FilerInputStream(input.getAnswer().response.getInputStream()));
                for (FilerInputStream fis : streams) {
                    return take(fis, highwaters, stream, intLongBuffer);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            },
            awaitLeaderElectionForNMillis,
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis);
    }

    @Override
    public TakeResult takePrefixFromTransactionId(List<RingMember> membersInOrder,
        byte[] prefix,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        TxKeyValueStream stream,
        long additionalSolverAfterNMillis,
        long abandonSolutionAfterNMillis,
        Optional<List<String>> solutionLog) throws Exception {
        byte[] intLongBuffer = new byte[8];
        return partitionCallRouter.take(solutionLog.orElse(null), partitionName, membersInOrder, "takePrefixFromTransactionId",
            (leader, ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/takePrefixFromTransactionId/" + base64PartitionName,
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, prefix, 0, prefix.length, "prefix", intLongBuffer);
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);

                return new PartitionResponse<>(new CloseableHttpStreamResponse(got), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            },
            (answers) -> {
                List<FilerInputStream> streams = Lists.transform(answers, input -> new FilerInputStream(input.getAnswer().response.getInputStream()));
                for (FilerInputStream fis : streams) {
                    return take(fis, highwaters, stream, intLongBuffer);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            },
            awaitLeaderElectionForNMillis,
            additionalSolverAfterNMillis,
            abandonSolutionAfterNMillis);
    }

    private TakeResult take(FilerInputStream fis, Highwaters highwaters, TxKeyValueStream stream, byte[] intLongBuffer) throws Exception {
        long maxTxId = -1;
        RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(fis, "ringMember", intLongBuffer));
        boolean done = false;

        while (!UIO.readBoolean(fis, "eos")) {
            RowType rowType = RowType.fromByte(UIO.readByte(fis, "type"));
            if (rowType == RowType.highwater) {
                highwaters.highwater(readHighwaters(fis, intLongBuffer));
            } else if (rowType == RowType.primary) {
                long rowTxId = UIO.readLong(fis, "rowTxId", intLongBuffer);
                if (done && rowTxId > maxTxId) {
                    return new TakeResult(ringMember, maxTxId, null);
                }
                done |= !stream.stream(rowTxId,
                    UIO.readByteArray(fis, "prefix", intLongBuffer),
                    UIO.readByteArray(fis, "key", intLongBuffer),
                    UIO.readByteArray(fis, "value", intLongBuffer),
                    UIO.readLong(fis, "timestampId", intLongBuffer),
                    UIO.readBoolean(fis, "tombstoned"),
                    UIO.readLong(fis, "version", intLongBuffer));
                maxTxId = Math.max(maxTxId, rowTxId);
            }
        }

        return new TakeResult(RingMember.fromBytes(UIO.readByteArray(fis, "ringMember", intLongBuffer)),
            UIO.readLong(fis, "lastTxId", intLongBuffer),
            readHighwaters(fis, intLongBuffer));
    }

    private WALHighwater readHighwaters(FilerInputStream inputStream, byte[] intLongBuffer) throws Exception {
        List<RingMemberHighwater> walHighwaters = new ArrayList<>();
        int length = UIO.readInt(inputStream, "length", intLongBuffer);
        for (int i = 0; i < length; i++) {
            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(inputStream, "ringMember", intLongBuffer));
            long txId = UIO.readLong(inputStream, "txId", intLongBuffer);
            walHighwaters.add(new RingMemberHighwater(ringMember, txId));
        }
        return new WALHighwater(walHighwaters);
    }

}
