package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.Consistency;
import com.jivesoftware.os.amza.api.PartitionClient;
import com.jivesoftware.os.amza.api.filer.FilerInputStream;
import com.jivesoftware.os.amza.api.filer.FilerOutputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.scan.Commitable;
import com.jivesoftware.os.amza.api.scan.RowType;
import com.jivesoftware.os.amza.api.scan.Scan;
import com.jivesoftware.os.amza.api.stream.TimestampKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.api.take.Highwaters;
import com.jivesoftware.os.amza.api.take.TakeResult;
import com.jivesoftware.os.amza.api.wal.WALHighwater;
import com.jivesoftware.os.amza.api.wal.WALHighwater.RingMemberHighwater;
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
    private final HttpPartitionCallRouter partitionCallRouter;

    public AmzaHttpPartitionClient(PartitionName partitionName, HttpPartitionCallRouter partitionCallRouter) throws IOException {
        this.base64PartitionName = partitionName.toBase64();
        this.partitionName = partitionName;
        this.partitionCallRouter = partitionCallRouter;
    }

    @Override
    public void commit(Consistency consistency, byte[] prefix, Commitable updates, long timeoutInMillis) throws Exception {
        partitionCallRouter.write(partitionName, consistency, "commit",
            (ringMember, client) -> {
                HttpResponse response = client.postStreamableRequest("/amza/v1/commit/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, prefix, "prefix");
                        UIO.writeLong(fos, timeoutInMillis, "timeoutInMillis");

                        updates.commitable(
                            (highwater) -> {
                            },
                            (rowTxId, key, value, valueTimestamp, valueTombstoned) -> {
                                UIO.writeBoolean(fos, false, "eos");
                                UIO.writeLong(fos, rowTxId, "rowTxId");
                                UIO.writeByteArray(fos, key, "key");
                                UIO.writeByteArray(fos, value, "value");
                                UIO.writeLong(fos, valueTimestamp, "valueTimestamp");
                                UIO.writeBoolean(fos, valueTombstoned, "valueTombstoned");
                                return true;
                            });
                        UIO.writeBoolean(fos, true, "eos");
                    }, null);
                return new PartitionCall.PartitionResponse<>(response, true);
            }, (answers) -> {
                return true;
            });
    }

    @Override
    public boolean get(Consistency consistency, byte[] prefix, UnprefixedWALKeys keys, TimestampKeyValueStream valuesStream) throws Exception {
        partitionCallRouter.read(partitionName, consistency, "get",
            (ringMember, client) -> {
                HttpStreamResponse response = client.streamingPostStreamableRequest("/amza/v1/get/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, prefix, "prefix");
                        keys.consume((key) -> {
                            UIO.writeBoolean(fos, false, "eos");
                            UIO.writeByteArray(fos, key, "key");
                            return true;
                        });
                        UIO.writeBoolean(fos, true, "eos");
                    }, null);
                FilerInputStream fis = new FilerInputStream(response.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, true);
            }, (answers) -> {

                boolean eos = false;
                while (!eos) {
                    byte[] latestPrefix;
                    byte[] latestKey;
                    byte[] latestValue;
                    long latestTimestamp = -1;
                    for (RingHostAnswer<FilerInputStream> answer : answers) {
                        FilerInputStream fis = answer.getAnswer();
                        if (!UIO.readBoolean(fis, "eos")) {
                            byte[] p = UIO.readByteArray(fis, "prefix");
                            byte[] k = UIO.readByteArray(fis, "key");
                            byte[] v = UIO.readByteArray(fis, "value");
                            long t = UIO.readLong(fis, "timestamp");

                            if (t > latestTimestamp) {
                                latestPrefix = p;
                                latestKey = k;
                                latestValue = v;
                                latestTimestamp = t;
                            }
                        } else {
                            eos = true;
                        }
                    }
                    valuesStream.stream(prefix, prefix, prefix, latestTimestamp);
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
        Scan stream) throws Exception {
        return partitionCallRouter.read(partitionName, consistency, "scan",
            (ringMember, client) -> {
                HttpStreamResponse response = client.streamingPostStreamableRequest("/amza/v1/scan/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, fromPrefix, "fromPrefix");
                        UIO.writeByteArray(fos, fromKey, "fromKey");
                        UIO.writeByteArray(fos, toPrefix, "toPrefix");
                        UIO.writeByteArray(fos, toKey, "toKey");
                    }, null);

                FilerInputStream fis = new FilerInputStream(response.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, true);
            }, (answers) -> {
                for (RingHostAnswer<FilerInputStream> answer : answers) {
                    FilerInputStream fis = answer.getAnswer();
                    while (!UIO.readBoolean(fis, "eos")) {
                        if (!stream.row(
                            UIO.readLong(fis, "rowTxId"),
                            UIO.readByteArray(fis, "prefix"),
                            UIO.readByteArray(fis, "key"),
                            UIO.readByteArray(fis, "value"),
                            UIO.readLong(fis, "timestampId")
                        )) {
                            return false;
                        }
                    }
                }
                return true;
            });
    }

    @Override
    public TakeResult takeFromTransactionId(Consistency consistency, Map<RingMember, Long> membersTxId, Highwaters highwaters, Scan scan) throws Exception {
        return partitionCallRouter.read(partitionName, consistency, "takeFromTransactionId",
            (ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse response = client.streamingPost(
                    "/amza/v1/takeFromTransactionId/" + base64PartitionName + "/" + consistency.name() + "/" + transactionId, null, null);
                FilerInputStream fis = new FilerInputStream(response.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, true);
            }, (answers) -> {
                for (RingHostAnswer<FilerInputStream> answer : answers) {
                    return take(answer.getAnswer(), highwaters, scan);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            });
    }

    @Override
    public TakeResult takePrefixFromTransactionId(Consistency consistency,
        byte[] prefix,
        Map<RingMember, Long> membersTxId,
        Highwaters highwaters,
        Scan scan) throws Exception {
        return partitionCallRouter.read(partitionName, consistency, "takePrefixFromTransactionId",
            (ringMember, client) -> {
                long transactionId = membersTxId.getOrDefault(ringMember, -1L);
                HttpStreamResponse response = client.streamingPostStreamableRequest(
                    "/amza/v1/takePrefixFromTransactionId/" + base64PartitionName + "/" + consistency.name(),
                    (out) -> {
                        FilerOutputStream fos = new FilerOutputStream(out);
                        UIO.writeByteArray(fos, prefix, "prefix");
                        UIO.writeLong(fos, transactionId, "transactionId");
                    }, null);
                FilerInputStream fis = new FilerInputStream(response.getInputStream());
                return new PartitionCall.PartitionResponse<>(fis, true);
            }, (answers) -> {
                for (RingHostAnswer<FilerInputStream> answer : answers) {
                    return take(answer.getAnswer(), highwaters, scan);
                }
                throw new RuntimeException("Failed to takePrefixFromTransactionId.");
            });
    }

    private TakeResult take(FilerInputStream fis, Highwaters highwaters, Scan scan) throws Exception {
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
                done |= !scan.row(rowTxId,
                    UIO.readByteArray(fis, "prefix"),
                    UIO.readByteArray(fis, "key"),
                    UIO.readByteArray(fis, "value"),
                    UIO.readLong(fis, "timestampId"));
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
