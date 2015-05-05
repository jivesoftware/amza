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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

final public class HttpClientConfig implements HttpClientConfiguration {

    private final int socketTimeoutInMillis;
    private final int maxConnections;
    private final int maxConnectionsPerHost;
    private final Map<String, String> headersForEveryRequest;

    private HttpClientConfig(int socketTimeoutInMillis, int maxConnections, int maxConnectionsPerHost,
        Map<String, String> headersForEveryRequest) {
        this.socketTimeoutInMillis = socketTimeoutInMillis;
        this.maxConnections = maxConnections;
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        this.headersForEveryRequest = new HashMap<>(headersForEveryRequest);
    }

    public int getSocketTimeoutInMillis() {
        return socketTimeoutInMillis;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public Map<String, String> getCopyOfHeadersForEveryRequest() {
        return new HashMap<>(headersForEveryRequest);
    }

    @Override
    public String toString() {
        return "HttpClientConfig{" + "socketTimeoutInMillis=" + socketTimeoutInMillis + ", maxConnections="
            + maxConnections + ", maxConnectionsPerHost=" + maxConnectionsPerHost + ", headersForEveryRequest="
            + headersForEveryRequest + '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    final public static class Builder {

        private int socketTimeoutInMillis = -1;
        private int maxConnections = -1;
        private int maxConnectionsPerHost = -1;
        private Map<String, String> headersForEveryRequest = Collections.emptyMap();

        private Builder() {
        }

        public Builder setSocketTimeoutInMillis(int socketTimeoutInMillis) {
            this.socketTimeoutInMillis = socketTimeoutInMillis;
            return this;
        }

        public Builder setMaxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public Builder setMaxConnectionsPerHost(int maxConnectionsPerHost) {
            this.maxConnectionsPerHost = maxConnectionsPerHost;
            return this;
        }

        public Builder setHeadersForEveryRequest(Map<String, String> headersForEveryRequest) {
            this.headersForEveryRequest = headersForEveryRequest;
            return this;
        }

        public HttpClientConfig build() {
            return new HttpClientConfig(
                socketTimeoutInMillis, maxConnections, maxConnectionsPerHost,
                headersForEveryRequest);
        }
    }
}
