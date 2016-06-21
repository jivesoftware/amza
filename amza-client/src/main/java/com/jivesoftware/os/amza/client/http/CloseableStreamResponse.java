package com.jivesoftware.os.amza.client.http;

import java.io.InputStream;

/**
 *
 * @author jonathan.colt
 */
public interface CloseableStreamResponse extends Closeable {

    InputStream getInputStream();

    long getActiveCount();
}
