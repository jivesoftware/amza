package org.glassfish.jersey.server;

import com.jivesoftware.os.amza.api.filer.ICloseable;
import com.jivesoftware.os.amza.api.filer.IReadable;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.ring.RingHost;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.ring.TimestampedRingHost;
import com.jivesoftware.os.amza.service.replication.http.endpoints.ChunkedOutputFiler;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.container.ConnectionCallback;
import org.glassfish.jersey.internal.util.collection.Value;
import org.glassfish.jersey.process.internal.RequestScope;
import org.glassfish.jersey.process.internal.RequestScope.Instance;
import org.glassfish.jersey.server.internal.process.AsyncContext;

import static com.jivesoftware.os.amza.api.stream.RowType.system;

/**
 *
 */
public class LatchChunkedOutput extends ChunkedOutput<byte[]> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final CountDownLatch latch = new CountDownLatch(1);
    private final long timeoutMillis;

    public LatchChunkedOutput(long timeoutMillis) {
        super(byte[].class);
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    void setContext(RequestScope requestScope,
        Instance requestScopeInstance,
        ContainerRequest requestContext,
        ContainerResponse responseContext,
        ConnectionCallback connectionCallbackRunner,
        Value<AsyncContext> asyncContext) throws IOException {
        super.setContext(requestScope, requestScopeInstance, requestContext, responseContext, connectionCallbackRunner, asyncContext);
        latch.countDown();
    }

    public void await(String context, Callable<Void> callable) {
        try {
            if (latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                callable.call();
            } else {
                LOG.error("Timeout awaiting chunk output for context:{}", context);
            }
        } catch (Exception x) {
            LOG.error("Failed during await for context:{}", new Object[] { context }, x);
        } finally {
            try {
                if (!isClosed()) {
                    close();
                }
            } catch (IOException x) {
                LOG.warn("Failed to close stream for context:{}", new Object[] { context }, x);
            }
        }
    }

    public interface ChunkCallable {
        void call(PartitionName partitionName, IReadable in, ChunkedOutputFiler out) throws Exception;
    }

    public void submit(ExecutorService executorService, PartitionName partitionName, String context, IReadable in, int bufferSize, ChunkCallable callable) {
        executorService.submit(() -> {
            ChunkedOutputFiler out = null;
            try {
                if (latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                    out = new ChunkedOutputFiler(bufferSize, this);
                    callable.call(partitionName, in, out);
                    out.flush(true);
                } else {
                    LOG.error("Timeout submitting chunk output for partition:{} context:{}", partitionName, context);
                }
            } catch (Exception x) {
                LOG.error("Failed during submit for partition:{} context:{}", new Object[] { partitionName, context }, x);
            } finally {
                closeStreams(partitionName, context, in, out);
            }
        });
    }

    private void closeStreams(PartitionName partitionName, String context, ICloseable in, ICloseable out) {
        if (in != null) {
            try {
                in.close();
            } catch (Exception x) {
                LOG.error("Failed to close input stream for partition:{} context:{}", new Object[] { partitionName, context }, x);
            }
        }
        if (out != null) {
            try {
                out.close();
            } catch (Exception x) {
                LOG.error("Failed to close output stream for partition:{} context:{}", new Object[] { partitionName, context }, x);
            }
        }
    }
}
