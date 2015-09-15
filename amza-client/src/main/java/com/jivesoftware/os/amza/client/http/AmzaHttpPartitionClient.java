package com.jivesoftware.os.amza.client.http;

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
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpStatus;

/**
 * @author jonathan.colt
 */
public class AmzaHttpPartitionClient implements PartitionClient {

    private final String base64PartitionName;
    private final PartitionName partitionName;
    private final AmzaHttpClientCallRouter partitionCallRouter;

    public AmzaHttpPartitionClient(PartitionName partitionName, AmzaHttpClientCallRouter partitionCallRouter) throws IOException {
        this.base64PartitionName = partitionName.toBase64();
        this.partitionName = partitionName;
        this.partitionCallRouter = partitionCallRouter;
    }

    private void handleLeaderStatusCodes(Consistency consistency, int statusCode) {
        if (consistency.requiresLeader()) {
            if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                partitionCallRouter.invalidateRouting(partitionName);
                throw new LeaderElectionInProgressException(partitionName + " " + consistency);
            }
            if (statusCode == HttpStatus.SC_CONFLICT) {
                partitionCallRouter.invalidateRouting(partitionName);
                throw new NoLongerTheLeaderException(partitionName + " " + consistency);
            }
        }
    }

    @Override
    public void commit(Consistency consistency, byte[] prefix, ClientUpdates updates, long timeoutInMillis) throws Exception {
        partitionCallRouter.write(partitionName, consistency, "commit",
            (leader, ringMember, client) -> {
                boolean checkLeader = ringMember.equals(leader);
                HttpResponse got = client.postStreamableRequest("/amza/v1/commit/" + base64PartitionName + "/" + consistency.name() + "/" + checkLeader,
                    (out) -> {
                        try {
                            FilerOutputStream fos = new FilerOutputStream(out);
                            UIO.writeByteArray(fos, prefix, "prefix");
                            UIO.writeLong(fos, timeoutInMillis, "timeoutInMillis");

                            updates.updates((rowTxId, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
                                UIO.writeBoolean(fos, false, "eos");
                                UIO.writeLong(fos, rowTxId, "rowTxId");
                                UIO.writeByteArray(fos, key, "key");
                                UIO.writeByteArray(fos, value, "value");
                                UIO.writeLong(fos, valueTimestamp, "valueTimestamp");
                                UIO.writeBoolean(fos, valueTombstoned, "valueTombstoned");
                                // valueVersion is only ever generated on the servers.
                                return true;
                            });
                            UIO.writeBoolean(fos, true, "eos");
                        } catch (Exception x) {
                            throw new RuntimeException("Failed while streaming commitable.", x);
                        }
                    }, null);

                handleLeaderStatusCodes(consistency, got.getStatusCode());
                return new PartitionResponse<>(got, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                return true;
            }, timeoutInMillis);
    }

    @Override
    public boolean get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, KeyValueTimestampStream valuesStream) throws Exception {
        partitionCallRouter.read(partitionName, consistency, "get",
            (leader, ringMember, client) -> {
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/get/" + base64PartitionName + "/" + consistency.name() + "/" + ringMember.equals(leader),
                    (out) -> {
                        try {
                            FilerOutputStream fos = new FilerOutputStream(out);
                            UIO.writeByteArray(fos, prefix, "prefix");
                            keys.consume((key) -> {
                                UIO.writeBoolean(fos, false, "eos");
                                UIO.writeByteArray(fos, key, "key");
                                return true;
                            });
                            UIO.writeBoolean(fos, true, "eos");
                        } catch (Exception x) {
                            throw new RuntimeException("Failed while streaming keys.", x);
                        }
                    }, null);
                handleLeaderStatusCodes(consistency, got.getStatusCode());
                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {

                int eosed = 0;
                while (answers.size() > 0 && eosed == 0) {
                    byte[] latestPrefix = null;
                    byte[] latestKey = null;
                    byte[] latestValue = null;
                    long latestTimestamp = Long.MIN_VALUE;
                    long latestVersion = Long.MIN_VALUE;
                    for (RingMemberAndHostAnswer<FilerInputStream> answer : answers) {
                        FilerInputStream fis = answer.getAnswer();
                        if (!UIO.readBoolean(fis, "eos")) {
                            byte[] p = UIO.readByteArray(fis, "prefix");
                            byte[] k = UIO.readByteArray(fis, "key");
                            byte[] v = UIO.readByteArray(fis, "value");
                            long t = UIO.readLong(fis, "timestamp");
                            boolean d = UIO.readBoolean(fis, "tombstone");
                            long z = UIO.readLong(fis, "version");

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
            });
        return true;
    }

    @Override
    public boolean scan(Consistency consistency,
        byte[] fromPrefix,
        byte[] fromKey,
        byte[] toPrefix,
        byte[] toKey,
        KeyValueTimestampStream stream) throws Exception {
        boolean merge;
        if (consistency == Consistency.leader_plus_one
            || consistency == Consistency.leader_quorum
            || consistency == Consistency.quorum
            || consistency == Consistency.write_one_read_all) {
            merge = true;
        } else {
            merge = false;
        }
        return partitionCallRouter.read(partitionName, consistency, "scan",
            (leader, ringMember, client) -> {
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/scan/" + base64PartitionName + "/" + consistency.name() + "/" + ringMember.equals(leader),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, fromPrefix, "fromPrefix");
                        UIO.writeByteArray(fos, fromKey, "fromKey");
                        UIO.writeByteArray(fos, toPrefix, "toPrefix");
                        UIO.writeByteArray(fos, toKey, "toKey");
                    }, null);

                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                int size = answers.size();
                if (merge && size > 1) {
                    boolean[] eos = new boolean[size];
                    QuorumScan quorumScan = new QuorumScan(size);
                    int eosed = 0;
                    while (eosed < size) {
                        for (int i = 0; i < size; i++) {
                            if (!quorumScan.used(i) && !eos[i]) {
                                FilerInputStream fis = answers.get(i).getAnswer();
                                eos[i] = UIO.readBoolean(fis, "eos");
                                if (!eos[i]) {
                                    quorumScan.fill(i, UIO.readByteArray(fis, "prefix"),
                                        UIO.readByteArray(fis, "key"),
                                        UIO.readByteArray(fis, "value"),
                                        UIO.readLong(fis, "timestampId"),
                                        UIO.readLong(fis, "version"));
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
                    return true;

                } else {
                    for (RingMemberAndHostAnswer<FilerInputStream> answer : answers) {
                        FilerInputStream fis = answer.getAnswer();
                        while (!UIO.readBoolean(fis, "eos")) {
                            if (!stream.stream(UIO.readByteArray(fis, "prefix"),
                                UIO.readByteArray(fis, "key"),
                                UIO.readByteArray(fis, "value"),
                                UIO.readLong(fis, "timestampId"),
                                UIO.readLong(fis, "version"))) {
                                return false;
                            }
                        }
                        return true;
                    }
                }
                throw new RuntimeException("Failed to scan.");
            });
    }

    @Override
    public TakeResult takeFromTransactionId(List<RingMember> membersInOrder,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {
        return partitionCallRouter.take(partitionName, membersInOrder, "takeFromTransactionId",
            (leader, ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/takeFromTransactionId/" + base64PartitionName,
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);

                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                for (RingMemberAndHostAnswer<FilerInputStream> answer : answers) {
                    return take(answer.getAnswer(), highwaters, stream);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            });
    }

    @Override
    public TakeResult takePrefixFromTransactionId(List<RingMember> membersInOrder,
        byte[] prefix,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {
        return partitionCallRouter.take(partitionName, membersInOrder, "takePrefixFromTransactionId",
            (leader, ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/takePrefixFromTransactionId/" + base64PartitionName,
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, prefix, "prefix");
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);

                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                for (RingMemberAndHostAnswer<FilerInputStream> answer : answers) {
                    return take(answer.getAnswer(), highwaters, stream);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            });
    }

    private TakeResult take(FilerInputStream fis, Highwaters highwaters, TxKeyValueStream stream) throws Exception {
        long maxTxId = -1;
        RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(fis, "ringMember"));
        boolean done = false;

        while (!UIO.readBoolean(fis, "eos")) {
            RowType rowType = RowType.fromByte(UIO.readByte(fis, "type"));
            if (rowType == RowType.highwater) {
                highwaters.highwater(readHighwaters(fis));
            } else if (rowType == RowType.primary) {
                long rowTxId = UIO.readLong(fis, "rowTxId");
                if (done && rowTxId > maxTxId) {
                    return new TakeResult(ringMember, maxTxId, null);
                }
                done |= !stream.stream(rowTxId,
                    UIO.readByteArray(fis, "prefix"),
                    UIO.readByteArray(fis, "key"),
                    UIO.readByteArray(fis, "value"),
                    UIO.readLong(fis, "timestampId"),
                    UIO.readBoolean(fis, "tombstoned"),
                    UIO.readLong(fis, "version"));
                maxTxId = Math.max(maxTxId, rowTxId);
            }
        }

        return new TakeResult(RingMember.fromBytes(UIO.readByteArray(fis, "ringMember")),
            UIO.readLong(fis, "lastTxId"),
            readHighwaters(fis));
    }

    private WALHighwater readHighwaters(FilerInputStream inputStream) throws Exception {
        List<RingMemberHighwater> walHighwaters = new ArrayList<>();
        int length = UIO.readInt(inputStream, "length");
        for (int i = 0; i < length; i++) {
            RingMember ringMember = RingMember.fromBytes(UIO.readByteArray(inputStream, "ringMember"));
            long txId = UIO.readLong(inputStream, "txId");
            walHighwaters.add(new RingMemberHighwater(ringMember, txId));
        }
        return new WALHighwater(walHighwaters);
    }

}
