package com.jivesoftware.os.amza.transport.http.replication.client;

import java.io.InputStream;

public class HttpStreamResponse {

    protected final int statusCode;
    protected final String statusReasonPhrase;
    InputStream inputStream;

    public HttpStreamResponse(int statusCode, String statusReasonPhrase, InputStream inputStream) {
        this.statusCode = statusCode;
        this.statusReasonPhrase = statusReasonPhrase;
        this.inputStream = inputStream;
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
}
