/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.transport.http.replication.client;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

class ApacheHttpClient441BackedHttpClient implements HttpClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger(true);

    private static final int JSON_POST_LOG_LENGTH_LIMIT = 2048;
    private static final String TIMER_NAME = "OutboundHttpRequest";

    public static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    public static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";
    public static final String APPLICATION_OCTET_STREAM_TYPE = "application/octet-stream";

    private final org.apache.http.client.HttpClient client;
    private final Map<String, String> headersForEveryRequest;

    public ApacheHttpClient441BackedHttpClient(org.apache.http.client.HttpClient client,
        Map<String, String> headersForEveryRequest) {
        this.client = client;
        this.headersForEveryRequest = headersForEveryRequest;
    }

    @Override
    public HttpStreamResponse streamingPost(String path, String postJsonBody, Map<String, String> headers) throws HttpClientException {
        return executePostJsonStreamingResponse(new HttpPost(path), postJsonBody, headers);
    }

    private String clientToString() {
        return client.toString(); //TODO
    }

    private HttpStreamResponse executePostJsonStreamingResponse(HttpEntityEnclosingRequestBase requestBase, String jsonBody, Map<String, String> headers)
        throws HttpClientException {
        try {
            setRequestHeaders(headers, requestBase);

            requestBase.setEntity(new StringEntity(jsonBody, ContentType.APPLICATION_JSON));
            requestBase.addHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON_CONTENT_TYPE);
            return executeStream(requestBase);
        } catch (Exception e) {
            String trimmedMethodBody = (jsonBody.length() > JSON_POST_LOG_LENGTH_LIMIT)
                ? jsonBody.substring(0, JSON_POST_LOG_LENGTH_LIMIT) : jsonBody;
            throw new HttpClientException("Error executing " + requestBase.getMethod() + " request to: "
                + clientToString() + " path: " + requestBase.getURI().getPath() + " JSON body: " + trimmedMethodBody, e);
        }
    }

    private HttpStreamResponse executeStream(HttpRequestBase requestBase) throws Exception {

        applyHeadersCommonToAllRequests(requestBase);

        org.apache.http.HttpResponse response = client.execute(requestBase);
        StatusLine statusLine = response.getStatusLine();
        checkStreamStatus(statusLine);
        return new HttpStreamResponse(statusLine.getStatusCode(), statusLine.getReasonPhrase(), response.getEntity().getContent(), requestBase);
    }

    private void checkStreamStatus(StatusLine statusLine) throws HttpClientException {
        int status = statusLine.getStatusCode();
        LOG.debug("Got status: {} {}", status, statusLine.getReasonPhrase());
        if (status < 200 || status >= 300) {
            throw new HttpClientException("Bad status : " + statusLine);
        }
    }

    @Override
    public HttpResponse get(String path, Map<String, String> headers) throws HttpClientException {
        HttpGet get = new HttpGet(path);

        setRequestHeaders(headers, get);

        try {
            return execute(get);
        } catch (Exception e) {
            throw new HttpClientException("Error executing GET request to: " + clientToString()
                + " path: " + path, e);
        }
    }

    @Override
    public HttpResponse postJson(String path, String postJsonBody, Map<String, String> headers) throws HttpClientException {
        try {
            HttpPost post = new HttpPost(path);

            setRequestHeaders(headers, post);

            post.setEntity(new StringEntity(postJsonBody, ContentType.APPLICATION_JSON));
            post.setHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON_CONTENT_TYPE);
            return execute(post);
        } catch (Exception e) {
            String trimmedPostBody = (postJsonBody.length() > JSON_POST_LOG_LENGTH_LIMIT)
                ? postJsonBody.substring(0, JSON_POST_LOG_LENGTH_LIMIT) : postJsonBody;
            throw new HttpClientException("Error executing POST request to: "
                + clientToString() + " path: " + path + " JSON body: " + trimmedPostBody, e);
        }
    }

    @Override
    public String toString() {
        return "ApacheHttpClient441BackedHttpClient{"
            + "client=" + client
            + ", headersForEveryRequest=" + headersForEveryRequest
            + '}';
    }

    private HttpResponse execute(HttpRequestBase requestBase) throws IOException {

        applyHeadersCommonToAllRequests(requestBase);

        byte[] responseBody;
        StatusLine statusLine = null;
        if (LOG.isInfoEnabled()) {
            LOG.startTimer(TIMER_NAME);
        }
        try {

            org.apache.http.HttpResponse response = client.execute(requestBase);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            InputStream responseBodyAsStream = response.getEntity().getContent();
            if (responseBodyAsStream != null) {
                IOUtils.copy(responseBodyAsStream, outputStream);
            }

            responseBody = outputStream.toByteArray();
            statusLine = response.getStatusLine();

            return new HttpResponse(statusLine.getStatusCode(), statusLine.getReasonPhrase(), responseBody);

        } finally {
            requestBase.releaseConnection();
            if (LOG.isInfoEnabled()) {
                long elapsedTime = LOG.stopTimer(TIMER_NAME);
                StringBuilder httpInfo = new StringBuilder();
                if (statusLine != null) {
                    httpInfo.append("Outbound ").append(statusLine.getProtocolVersion()).append(" Status ").append(statusLine.getStatusCode());
                } else {
                    httpInfo.append("Exception sending request");
                }
                httpInfo.append(" in ").append(elapsedTime).append(" ms ").append(requestBase.getMethod()).append(" ")
                    .append(client).append(requestBase.getURI());
                LOG.debug(httpInfo.toString());
            }
        }
    }

    private void setRequestHeaders(Map<String, String> headers, HttpRequestBase requestBase) {
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                requestBase.setHeader(header.getKey(), header.getValue());
            }
        }
    }

    private void applyHeadersCommonToAllRequests(HttpRequestBase requestBase) {
        for (Map.Entry<String, String> headerEntry : headersForEveryRequest.entrySet()) {
            requestBase.setHeader(headerEntry.getKey(), headerEntry.getValue());
        }
    }

}
