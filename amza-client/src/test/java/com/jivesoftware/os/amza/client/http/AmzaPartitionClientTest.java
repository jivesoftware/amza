package com.jivesoftware.os.amza.client.http;

import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.amza.api.PartitionClient.KeyValueFilter;
import com.jivesoftware.os.amza.api.filer.FilerOutputStream;
import com.jivesoftware.os.amza.api.filer.IWriteable;
import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.RingMemberAndHost;
import com.jivesoftware.os.amza.api.stream.ClientUpdates;
import com.jivesoftware.os.amza.api.stream.OffsetUnprefixedWALKeys;
import com.jivesoftware.os.amza.api.stream.PrefixedKeyRanges;
import com.jivesoftware.os.amza.api.stream.UnprefixedWALKeys;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;
import org.xerial.snappy.SnappyOutputStream;

/**
 *
 */
public class AmzaPartitionClientTest {

    @Test
    public void testScan() throws Exception {
        byte[] partitionNameBytes = "abc".getBytes();
        PartitionName partitionName = new PartitionName(false, partitionNameBytes, partitionNameBytes);
        AmzaClientCallRouter<TestClient, Exception> router = new AmzaClientCallRouter<>(MoreExecutors.sameThreadExecutor(), new TestPartitionHostsProvider(3),
            new TestRingHostClientProvider());
        AmzaPartitionClient<TestClient, Exception> client = new AmzaPartitionClient<>(partitionName, router, new TestRemotePartitionCaller(),
            10_000L, -1, -1);

        client.scan(Consistency.quorum,
            false,
            stream -> stream.stream(null, null, null, null),
            (prefix, key, value, timestamp, version) -> {
                System.out.println("Got " + Arrays.toString(key) + " = " + Arrays.toString(value) + " @ " + timestamp);
                return true;
            },
            1_000L,
            10_000L,
            30_000L,
            Optional.<List<String>>empty());
    }

    @Test
    public void testScanKeys() throws Exception {
        byte[] partitionNameBytes = "abc".getBytes();
        PartitionName partitionName = new PartitionName(false, partitionNameBytes, partitionNameBytes);
        AmzaClientCallRouter<TestClient, Exception> router = new AmzaClientCallRouter<>(MoreExecutors.sameThreadExecutor(), new TestPartitionHostsProvider(3),
            new TestRingHostClientProvider());
        AmzaPartitionClient<TestClient, Exception> client = new AmzaPartitionClient<>(partitionName, router, new TestRemotePartitionCaller(),
            10_000L, -1, -1);

        client.scanKeys(Consistency.quorum,
            false,
            stream -> stream.stream(null, null, null, null),
            (prefix, key, value, timestamp, version) -> {
                System.out.println("Got " + Arrays.toString(key) + " = " + Arrays.toString(value) + " @ " + timestamp);
                return true;
            },
            1_000L,
            10_000L,
            30_000L,
            Optional.<List<String>>empty());
    }

    private class TestClient {

    }

    private class TestRingHostClientProvider implements RingHostClientProvider<TestClient, Exception> {

        @Override
        public <R> R call(PartitionName partitionName,
            RingMember leader,
            RingMemberAndHost ringMemberAndHost,
            String family,
            PartitionCall<TestClient, R, Exception> clientCall) throws Exception {

            return clientCall.call(leader, ringMemberAndHost.ringMember, new TestClient()).response;
        }
    }

    private class TestRemotePartitionCaller implements RemotePartitionCaller<TestClient, Exception> {

        @Override
        public PartitionResponse<NoOpCloseable> commit(RingMember leader,
            RingMember ringMember,
            TestClient client,
            Consistency consistency,
            byte[] prefix,
            ClientUpdates updates,
            long abandonSolutionAfterNMillis) throws Exception {
            throw new UnsupportedOperationException("blah");
        }

        @Override
        public PartitionResponse<CloseableStreamResponse> get(RingMember leader,
            RingMember ringMember,
            TestClient client,
            Consistency consistency,
            byte[] prefix,
            UnprefixedWALKeys keys) throws Exception {
            throw new UnsupportedOperationException("blah");
        }

        @Override
        public PartitionResponse<CloseableStreamResponse> getOffset(RingMember leader,
            RingMember ringMember,
            TestClient client,
            Consistency consistency,
            byte[] prefix,
            OffsetUnprefixedWALKeys keys) throws Exception {
            throw new UnsupportedOperationException("blah");
        }

        @Override
        public PartitionResponse<CloseableStreamResponse> scan(RingMember leader,
            RingMember ringMember,
            TestClient client,
            Consistency consistency,
            boolean compressed,
            PrefixedKeyRanges ranges,
            KeyValueFilter filter,
            boolean hydrateValues) throws Exception {

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            FilerOutputStream out = new FilerOutputStream(compressed ? new BufferedOutputStream(new SnappyOutputStream(bytesOut), 8192) : bytesOut);
            try {
                scanOut(out, 10, hydrateValues);
                out.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            byte[] serialized = bytesOut.toByteArray();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);

            return new PartitionResponse<>(new CloseableStreamResponse() {
                @Override
                public InputStream getInputStream() {
                    return inputStream;
                }

                @Override
                public long getActiveCount() {
                    return 0;
                }

                @Override
                public void abort() throws Exception {
                }

                @Override
                public void close() throws Exception {
                }
            }, true);
        }

        private void scanOut(IWriteable out, int count, boolean hydrateValues) throws Exception {
            byte[] intLongBuffer = new byte[8];
            for (int i = 0; i < count; i++) {
                UIO.writeByte(out, (byte) 0, "eos");
                UIO.writeByteArray(out, null, "prefix", intLongBuffer);
                UIO.writeByteArray(out, UIO.intBytes(i), "key", intLongBuffer);
                if (hydrateValues) {
                    UIO.writeByteArray(out, UIO.intBytes(-i), "value", intLongBuffer);
                }
                UIO.writeLong(out, 1_000 + i, "timestampId", intLongBuffer);
                UIO.writeLong(out, 2_000 + i, "version", intLongBuffer);
            }
            UIO.writeByte(out, (byte) 1, "eos");
            out.flush(false);
        }

        @Override
        public PartitionResponse<CloseableStreamResponse> takeFromTransactionId(RingMember leader,
            RingMember ringMember,
            TestClient client,
            Map<RingMember, Long> membersTxId,
            int limit) throws Exception {
            throw new UnsupportedOperationException("blah");
        }

        @Override
        public PartitionResponse<CloseableStreamResponse> takePrefixFromTransactionId(RingMember leader,
            RingMember ringMember,
            TestClient client,
            byte[] prefix,
            Map<RingMember, Long> membersTxId,
            int limit) throws Exception {
            throw new UnsupportedOperationException("blah");
        }

        @Override
        public PartitionResponse<CloseableLong> getApproximateCount(RingMember leader, RingMember ringMember, TestClient client) throws Exception {
            throw new UnsupportedOperationException("blah");
        }
    }
}
