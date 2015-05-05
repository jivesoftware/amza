package com.jivesoftware.os.amza.transport.tcp.replication.shared;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class BufferProviderTest {

    @Test
    public void testAcquireRelease() {
        BufferProvider bufferProvider = new BufferProvider(1024, 2, true, 500);

        List<ByteBuffer> buffers = new ArrayList<>();
        ByteBuffer acquired = bufferProvider.acquire();

        Assert.assertNotNull(acquired);
        Assert.assertTrue(acquired.isDirect());
        buffers.add(acquired);

        acquired = bufferProvider.acquire();

        Assert.assertNotNull(acquired);
        Assert.assertTrue(acquired.isDirect());
        buffers.add(acquired);

        long start = System.currentTimeMillis();
        acquired = bufferProvider.acquire();
        long elapsed = System.currentTimeMillis() - start;

        Assert.assertTrue(elapsed >= 500);

        Assert.assertNotNull(acquired);
        Assert.assertFalse(acquired.isDirect());
        buffers.add(acquired);

        IdentityHashMap<ByteBuffer, Boolean> identities = new IdentityHashMap<>();

        for (ByteBuffer buffer : buffers) {
            identities.put(buffer, Boolean.TRUE);
            bufferProvider.release(buffer);
        }

        Assert.assertEquals(identities.size(), buffers.size());

    }
}
