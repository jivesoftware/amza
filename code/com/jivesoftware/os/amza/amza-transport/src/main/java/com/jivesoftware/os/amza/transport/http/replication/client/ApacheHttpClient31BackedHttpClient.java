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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.IOUtils;

class ApacheHttpClient31BackedHttpClient implements HttpClient {

    public static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    public static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";
    public static final String APPLICATION_OCTET_STREAM_TYPE = "application/octet-stream";

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger(true);
    private static final String TIMER_NAME = "OutboundHttpRequest";
    private final org.apache.commons.httpclient.HttpClient client;
    private final Map<String, String> headersForEveryRequest;
    private String consumerKey;
    private static final int JSON_POST_LOG_LENGTH_LIMIT = 2048;

    public ApacheHttpClient31BackedHttpClient(org.apache.commons.httpclient.HttpClient client,
        Map<String, String> headersForEveryRequest) {
        this.client = client;
        this.headersForEveryRequest = headersForEveryRequest;
    }

    @Override
    public HttpStreamResponse streamingPost(String path, String postJsonBody, Map<String, String> headers) throws HttpClientException {
        return executePostJsonStreamingResponse(new PostMethod(path), postJsonBody, headers);
    }

    private HttpStreamResponse executePostJsonStreamingResponse(EntityEnclosingMethod method, String jsonBody, Map<String, String> headers)
        throws HttpClientException {
        try {
            setRequestHeaders(headers, method);

            method.setRequestEntity(new StringRequestEntity(jsonBody, APPLICATION_JSON_CONTENT_TYPE, "UTF-8"));
            method.setRequestHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON_CONTENT_TYPE);
            return executeStream(method);
        } catch (Exception e) {
            String trimmedMethodBody = (jsonBody.length() > JSON_POST_LOG_LENGTH_LIMIT)
                ? jsonBody.substring(0, JSON_POST_LOG_LENGTH_LIMIT) : jsonBody;
            throw new HttpClientException("Error executing " + method.getName() + " request to: "
                + client.getHostConfiguration().getHostURL() + " path: " + method.getPath() + " JSON body: " + trimmedMethodBody, e);
        }
    }

    private HttpStreamResponse executeStream(HttpMethod method) throws Exception {

        applyHeadersCommonToAllRequests(method);

        int status = client.executeMethod(method);
        checkStreamStatus(status, method);
        StatusLine statusLine = method.getStatusLine();
        return new HttpStreamResponse(statusLine.getStatusCode(), statusLine.getReasonPhrase(), method.getResponseBodyAsStream());
    }

    private void checkStreamStatus(int status, HttpMethod httpMethod) throws HttpClientException {
        LOG.debug("Got status: {} {}", status, httpMethod.getStatusText());
        if (status < 200 || status >= 300) {
            throw new HttpClientException("Bad status : " + httpMethod.getStatusLine());
        }
    }

    @Override
    public HttpResponse get(String path, Map<String, String> headers, int timeoutMillis) throws HttpClientException {
        GetMethod get = new GetMethod(path);

        setRequestHeaders(headers, get);

        if (timeoutMillis > 0) {
            return executeWithTimeout(get, timeoutMillis);
        }
        try {
            return execute(get);
        } catch (Exception e) {
            throw new HttpClientException("Error executing GET request to: " + client.getHostConfiguration().getHostURL()
                + " path: " + path, e);
        }
    }

    private HttpResponse executeWithTimeout(final HttpMethodBase HttpMethod, int timeoutMillis) {
        client.getParams().setParameter("http.method.retry-handler", new DefaultHttpMethodRetryHandler(0, false));
        ExecutorService service = Executors.newSingleThreadExecutor();

        Future<HttpResponse> future = service.submit(new Callable<HttpResponse>() {
            @Override
            public HttpResponse call() throws IOException {
                return execute(HttpMethod);
            }
        });

        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            String uriInfo = "";
            try {
                uriInfo = " for " + HttpMethod.getURI();
            } catch (Exception ie) {
            }
            LOG.warn("Http connection thread was interrupted or has timed out" + uriInfo, e);
            return new HttpResponse(HttpStatus.SC_REQUEST_TIMEOUT, "Request Timeout", null);
        } finally {
            service.shutdownNow();
        }
    }

    @Override
    public HttpResponse postJson(String path, String postJsonBody, Map<String, String> headers, int timeoutMillis) throws HttpClientException {
        try {
            PostMethod post = new PostMethod(path);

            setRequestHeaders(headers, post);

            post.setRequestEntity(new StringRequestEntity(postJsonBody, APPLICATION_JSON_CONTENT_TYPE, "UTF-8"));
            post.setRequestHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON_CONTENT_TYPE);
            if (timeoutMillis > 0) {
                return executeWithTimeout(post, timeoutMillis);
            } else {
                return execute(post);
            }
        } catch (Exception e) {
            String trimmedPostBody = (postJsonBody.length() > JSON_POST_LOG_LENGTH_LIMIT)
                ? postJsonBody.substring(0, JSON_POST_LOG_LENGTH_LIMIT) : postJsonBody;
            throw new HttpClientException("Error executing POST request to: "
                + client.getHostConfiguration().getHostURL() + " path: " + path + " JSON body: " + trimmedPostBody, e);
        }
    }

    @Override
    public String toString() {
        return "ApacheHttpClient31BackedHttpClient{"
            + "client=" + client
            + ", headersForEveryRequest=" + headersForEveryRequest
            + ", consumerKey=" + consumerKey
            + '}';
    }

    private HttpResponse execute(HttpMethod method) throws IOException {

        applyHeadersCommonToAllRequests(method);

        byte[] responseBody;
        StatusLine statusLine = null;
        if (LOG.isInfoEnabled()) {
            LOG.startTimer(TIMER_NAME);
        }
        try {

            client.executeMethod(method);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            InputStream responseBodyAsStream = method.getResponseBodyAsStream();
            if (responseBodyAsStream != null) {
                IOUtils.copy(responseBodyAsStream, outputStream);
            }

            responseBody = outputStream.toByteArray();
            statusLine = method.getStatusLine();

            return new HttpResponse(statusLine.getStatusCode(), statusLine.getReasonPhrase(), responseBody);

        } finally {
            method.releaseConnection();
            if (LOG.isInfoEnabled()) {
                long elapsedTime = LOG.stopTimer(TIMER_NAME);
                StringBuilder httpInfo = new StringBuilder();
                if (statusLine != null) {
                    httpInfo.append("Outbound ").append(statusLine.getHttpVersion()).append(" Status ").append(statusLine.getStatusCode());
                } else {
                    httpInfo.append("Exception sending request");
                }
                httpInfo.append(" in ").append(elapsedTime).append(" ms ").append(method.getName()).append(" ")
                    .append(safeHostString(client.getHostConfiguration())).append(method.getURI());
                LOG.debug(httpInfo.toString());
            }
        }
    }

    private void setRequestHeaders(Map<String, String> headers, HttpMethodBase method) {
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                method.setRequestHeader(header.getKey(), header.getValue());
            }
        }
    }

    private void applyHeadersCommonToAllRequests(HttpMethod method) {
        for (Map.Entry<String, String> headerEntry : headersForEveryRequest.entrySet()) {
            method.addRequestHeader(headerEntry.getKey(), headerEntry.getValue());
        }
    }

    void setState(HttpState state) {
        client.setState(state);
    }

    void setHostConfiguration(HostConfiguration hostConfiguration) {
        client.setHostConfiguration(hostConfiguration);
    }

    private String safeHostString(HostConfiguration hostConfiguration) {
        if (hostConfiguration.getHost() != null) {
            return hostConfiguration.getHostURL();
        }
        return "";
    }

}
