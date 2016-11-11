package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.amza.api.FailedToAchieveQuorumException;
import com.jivesoftware.os.amza.api.filer.FilerOutputStream;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import com.jivesoftware.os.amza.client.http.exceptions.LeaderElectionInProgressException;
import com.jivesoftware.os.amza.client.http.exceptions.NoLongerTheLeaderException;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.http.HttpStatus;

/**
 *
 * @author jonathan.colt
 */
public class HttpRemotePartitionCaller implements RemotePartitionCaller<HttpClient, HttpClientException> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RouteInvalidator routeInvalidator;
    private final PartitionName partitionName;
    private final String base64PartitionName;

    public HttpRemotePartitionCaller(RouteInvalidator routeInvalidator,
        PartitionName partitionName) {
        this.routeInvalidator = routeInvalidator;
        this.partitionName = partitionName;
        this.base64PartitionName = partitionName.toBase64();
    }

    @Override
    public PartitionResponse<NoOpCloseable> commit(RingMember leader,
        RingMember ringMember,
        HttpClient client,
        Consistency consistency,
        byte[] prefix,
        ClientUpdates updates,
        long abandonSolutionAfterNMillis) throws HttpClientException {

        byte[] lengthBuffer = new byte[4];
        boolean checkLeader = ringMember.equals(leader);
        HttpResponse got = client.postStreamableRequest("/amza/v1/commit/" + base64PartitionName + "/" + consistency.name() + "/" + checkLeader,
            (out) -> {
                try {

                    FilerOutputStream fos = new FilerOutputStream(out);
                    UIO.writeByteArray(fos, prefix, "prefix", lengthBuffer);
                    UIO.writeLong(fos, abandonSolutionAfterNMillis, "timeoutInMillis");

                    updates.updates((key, value, valueTimestamp, valueTombstoned) -> {
                        UIO.write(fos, new byte[]{0}, "eos");
                        UIO.writeByteArray(fos, key, "key", lengthBuffer);
                        UIO.writeByteArray(fos, value, "value", lengthBuffer);
                        UIO.writeLong(fos, valueTimestamp, "valueTimestamp");
                        UIO.write(fos, new byte[]{valueTombstoned ? (byte) 1 : (byte) 0}, "valueTombstoned");
                        return true;
                    });
                    UIO.write(fos, new byte[]{1}, "eos");
                } catch (Exception x) {
                    throw new RuntimeException("Failed while streaming commitable.", x);
                } finally {
                    out.close();
                }
            }, null);

        if (got.getStatusCode() == Response.Status.ACCEPTED.getStatusCode()) {
            throw new FailedToAchieveQuorumException(
                "The server could NOT achieve " + consistency.name() + " within " + abandonSolutionAfterNMillis + "millis");
        }
        handleLeaderStatusCodes(consistency, got.getStatusCode(), got.getStatusReasonPhrase(), null);
        return new PartitionResponse<>(new NoOpCloseable(), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
    }

    @Override
    public PartitionResponse<CloseableStreamResponse> get(RingMember leader,
        RingMember ringMember,
        HttpClient client,
        Consistency consistency,
        byte[] prefix,
        UnprefixedWALKeys keys) throws HttpClientException {

        byte[] intLongBuffer = new byte[8];
        HttpStreamResponse got = client.streamingPostStreamableRequest(
            "/amza/v1/get/" + base64PartitionName + "/" + consistency.name() + "/" + ringMember.equals(leader),
            (out) -> {
                try {
                    FilerOutputStream fos = new FilerOutputStream(out);
                    UIO.writeByteArray(fos, prefix, "prefix", intLongBuffer);
                    keys.consume((key) -> {
                        UIO.write(fos, new byte[]{0}, "eos");
                        UIO.writeByteArray(fos, key, "key", intLongBuffer);
                        return true;
                    });
                    UIO.write(fos, new byte[]{1}, "eos");
                } catch (Exception x) {
                    throw new RuntimeException("Failed while streaming keys.", x);
                } finally {
                    out.close();
                }
            }, null);
        CloseableHttpStreamResponse closeableHttpStreamResponse = new CloseableHttpStreamResponse(got);
        handleLeaderStatusCodes(consistency, got.getStatusCode(), got.getStatusReasonPhrase(), closeableHttpStreamResponse);
        return new PartitionResponse<>(closeableHttpStreamResponse, got.getStatusCode() >= 200 && got.getStatusCode() < 300);
    }

    @Override
    public PartitionResponse<CloseableLong> getApproximateCount(RingMember leader, RingMember ringMember, HttpClient client) throws
        HttpClientException {
        HttpResponse got = client.get("/amza/v1/getApproximateCount/" + base64PartitionName + "/" + Consistency.none + "/" + ringMember.equals(leader), null);
        if (got.getStatusCode() >= 200 && got.getStatusCode() < 300) {
            return new PartitionResponse<>(new CloseableLong(Long.parseLong(new String(got.getResponseBody()))), true);
        } else {
            return new PartitionResponse<>(new CloseableLong(-1), false);
        }
    }

    @Override
    public PartitionResponse<CloseableStreamResponse> scan(RingMember leader,
        RingMember ringMember,
        HttpClient client,
        Consistency consistency,
        boolean compressed,
        PrefixedKeyRanges ranges,
        boolean hydrateValues) throws HttpClientException {

        byte[] intLongBuffer = new byte[8];

        String pathPrefix = compressed ? "/amza/v1/scanCompressed/" : "/amza/v1/scan/";
        HttpStreamResponse got = client.streamingPostStreamableRequest(
            pathPrefix + base64PartitionName + "/" + consistency.name() + "/" + ringMember.equals(leader) + "/" + hydrateValues,
            (out) -> {
                try {
                    FilerOutputStream fos = new FilerOutputStream(out);
                    ranges.consume((fromPrefix, fromKey, toPrefix, toKey) -> {
                        UIO.writeByte(fos, (byte) 1, "eos");
                        UIO.writeByteArray(fos, fromPrefix, "fromPrefix", intLongBuffer);
                        UIO.writeByteArray(fos, fromKey, "fromKey", intLongBuffer);
                        UIO.writeByteArray(fos, toPrefix, "toPrefix", intLongBuffer);
                        UIO.writeByteArray(fos, toKey, "toKey", intLongBuffer);
                        return true;
                    });
                    UIO.writeByte(fos, (byte) 0, "eos");
                } catch (Exception x) {
                    throw new RuntimeException("Failed while scanning ranges.", x);
                } finally {
                    out.close();
                }
            }, null);

        return new PartitionResponse<>(new CloseableHttpStreamResponse(got), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
    }

    @Override
    public PartitionResponse<CloseableStreamResponse> takeFromTransactionId(RingMember leader,
        RingMember ringMember,
        HttpClient client,
        Map<RingMember, Long> membersTxId) throws HttpClientException {

        long transactionId = membersTxId.getOrDefault(ringMember, -1L);
        HttpStreamResponse got = client.streamingPostStreamableRequest(
            "/amza/v1/takeFromTransactionId/" + base64PartitionName,
            (out) -> {
                try {
                    FilerOutputStream fos = new FilerOutputStream(out);
                    UIO.writeLong(fos, transactionId, "transactionId");
                } finally {
                    out.close();
                }
            }, null);

        return new PartitionResponse<>(new CloseableHttpStreamResponse(got), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
    }

    @Override
    public PartitionResponse<CloseableStreamResponse> takePrefixFromTransactionId(RingMember leader,
        RingMember ringMember,
        HttpClient client,
        byte[] prefix,
        Map<RingMember, Long> membersTxId) throws HttpClientException {

        byte[] intLongBuffer = new byte[8];

        long transactionId = membersTxId.getOrDefault(ringMember, -1L);
        HttpStreamResponse got = client.streamingPostStreamableRequest(
            "/amza/v1/takePrefixFromTransactionId/" + base64PartitionName,
            (out) -> {
                try {
                    FilerOutputStream fos = new FilerOutputStream(out);
                    UIO.writeByteArray(fos, prefix, "prefix", intLongBuffer);
                    UIO.writeLong(fos, transactionId, "transactionId");
                } finally {
                    out.close();
                }
            }, null);

        return new PartitionResponse<>(new CloseableHttpStreamResponse(got), got.getStatusCode() >= 200 && got.getStatusCode() < 300);
    }

    private void handleLeaderStatusCodes(Consistency consistency, int statusCode, String statusReasonPhrase, Closeable closeable) {
        if (statusCode == HttpStatus.SC_BAD_REQUEST) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception e) {
                LOG.warn("Failed to close {}", closeable);
            }
            throw new IllegalArgumentException("Bad request: " + statusReasonPhrase);
        } else if (consistency.requiresLeader()) {
            if (statusCode == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                try {
                    if (closeable != null) {
                        closeable.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to close {}", closeable);
                }
                routeInvalidator.invalidateRouting(partitionName);
                throw new LeaderElectionInProgressException(partitionName + " " + consistency);
            }
            if (statusCode == HttpStatus.SC_CONFLICT) {
                try {
                    if (closeable != null) {
                        closeable.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to close {}", closeable);
                }
                routeInvalidator.invalidateRouting(partitionName);
                throw new NoLongerTheLeaderException(partitionName + " " + consistency);
            }
        }
    }

}
