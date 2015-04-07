package com.jivesoftware.os.amza.transport.http.replication.client;

import java.io.InputStream;
import org.apache.http.client.methods.HttpRequestBase;

public class HttpStreamResponse {

    protected final int statusCode;
    protected final String statusReasonPhrase;
    protected InputStream inputStream;
    protected final HttpRequestBase requestBase;

    public HttpStreamResponse(int statusCode, String statusReasonPhrase, InputStream inputStream, HttpRequestBase requestBase) {
        this.statusCode = statusCode;
        this.statusReasonPhrase = statusReasonPhrase;
        this.inputStream = inputStream;
        this.requestBase = requestBase;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusReasonPhrase() {
        return statusReasonPhrase;
    }

    public void close() {
        requestBase.reset();
    }
}
