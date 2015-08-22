package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.CompareTimestampVersions;
import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.FilerOutputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.Commitable;
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

/**
 *
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

    @Override
    public void commit(Consistency consistency, byte[] prefix, Commitable updates, long timeoutInMillis) throws Exception {
        partitionCallRouter.write(partitionName, consistency, "commit",
            (leader, ringMember, client) -> {
                HttpResponse got = client.postStreamableRequest("/amza/v1/commit/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        try {
                            FilerOutputStream fos = new FilerOutputStream(out);
                            UIO.writeByteArray(fos, leader == null ? null : leader.toBytes(), "leader");
                            UIO.writeByteArray(fos, prefix, "prefix");
                            UIO.writeLong(fos, timeoutInMillis, "timeoutInMillis");

                            updates.commitable(
                                (highwater) -> {
                                },
                                (rowTxId, key, value, valueTimestamp, valueTombstoned, valueVersion) -> {
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
                return new PartitionCall.PartitionResponse<>(got, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                return true;
            }, timeoutInMillis);
    }

    private void handleLeaderStatusCodes(Consistency consistency, int statusCode) {
        if (consistency.requiresLeader()) {
            if (statusCode == 503) {
                throw new LeaderElectionInProgressException(partitionName + " " + consistency);
            }
            if (statusCode == 409) {
                throw new NoLongerTheLeaderException(partitionName + " " + consistency);
            }
        }
    }

    @Override
    public boolean get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, KeyValueTimestampStream valuesStream) throws Exception {
        partitionCallRouter.read(partitionName, consistency, "get",
            (leader, ringMember, client) -> {
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/get/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        try {
                            FilerOutputStream fos = new FilerOutputStream(out);
                            UIO.writeByteArray(fos, leader == null ? null : leader.toBytes(), "leader");
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
                return new PartitionCall.PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {

                boolean eos = false;
                while (!eos) {
                    byte[] latestPrefix = null;
                    byte[] latestKey = null;
                    byte[] latestValue = null;
                    long latestTimestamp = -1;
                    long latestVersion = -1;
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
                            eos = true;
                        }
                    }
                    valuesStream.stream(latestPrefix, latestKey, latestValue, latestTimestamp, latestVersion);
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
        return partitionCallRouter.read(partitionName, consistency, "scan",
            (leader, ringMember, client) -> {
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/scan/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, leader == null ? null : leader.toBytes(), "leader");
                        UIO.writeByteArray(fos, fromPrefix, "fromPrefix");
                        UIO.writeByteArray(fos, fromKey, "fromKey");
                        UIO.writeByteArray(fos, toPrefix, "toPrefix");
                        UIO.writeByteArray(fos, toKey, "toKey");
                    }, null);

                handleLeaderStatusCodes(consistency, got.getStatusCode());
                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                for (RingMemberAndHostAnswer<FilerInputStream> answer : answers) {
                    FilerInputStream fis = answer.getAnswer();
                    while (!UIO.readBoolean(fis, "eos")) {
                        if (!stream.stream(
                            UIO.readByteArray(fis, "prefix"),
                            UIO.readByteArray(fis, "key"),
                            UIO.readByteArray(fis, "value"),
                            UIO.readLong(fis, "timestampId"),
                            UIO.readLong(fis, "version")
                        )) {
                            return false;
                        }
                    }
                }
                return true;
            });
    }

    @Override
    public TakeResult takeFromTransactionId(Consistency consistency,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {
        return partitionCallRouter.read(partitionName, consistency, "takeFromTransactionId",
            (leader, ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/takeFromTransactionId/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, leader == null ? null : leader.toBytes(), "leader");
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);

                handleLeaderStatusCodes(consistency, got.getStatusCode());
                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
            }, (answers) -> {
                for (RingMemberAndHostAnswer<FilerInputStream> answer : answers) {
                    return take(answer.getAnswer(), highwaters, stream);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            });
    }

    @Override
    public TakeResult takePrefixFromTransactionId(Consistency consistency,
        byte[] prefix,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        TxKeyValueStream stream) throws Exception {
        return partitionCallRouter.read(partitionName, consistency, "takePrefixFromTransactionId",
            (leader, ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse got = client.streamingPostStreamableRequest(
                    "/amza/v1/takePrefixFromTransactionId/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, leader == null ? null : leader.toBytes(), "leader");
                        UIO.writeByteArray(fos, prefix, "prefix");
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);

                handleLeaderStatusCodes(consistency, got.getStatusCode());
                FilerInputStream fis = new FilerInputStream(got.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
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
