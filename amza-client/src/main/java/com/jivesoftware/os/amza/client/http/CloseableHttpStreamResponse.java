package com.jivesoftware.os.amza.client.http;

import com.jivesoftware.os.routing.bird.http.client.HttpStreamResponse;
import java.io.InputStream;

/**
 *
 * @author jonathan.colt
 */
public class CloseableHttpStreamResponse implements CloseableStreamResponse {

    private final HttpStreamResponse response;

    public CloseableHttpStreamResponse(HttpStreamResponse response) {
        this.response = response;
    }

    @Override
    public void abort() throws Exception {
        response.abort();
    }

    @Override
    public void close() throws Exception {
        response.close();
    }

    @Override
    public InputStream getInputStream() {
        return response.getInputStream();
    }

    @Override
    public long getActiveCount() {
        return response.getActiveCount();
    }

    @Override
    public String toString() {
        return "CloseableHttpStreamResponse{" +
            "statusCode=" + response.getStatusCode() +
            ", reasonPhrase='" + response.getStatusReasonPhrase() + '\'' +
            '}';
    }
}
