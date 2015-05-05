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

import java.util.Collection;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultRoutePlanner;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

public class HttpClientFactoryProvider {

    public HttpClientFactory createHttpClientFactory(final Collection<HttpClientConfiguration> configurations) {

        final HttpClientConfig httpClientConfig = locateConfig(configurations, HttpClientConfig.class, HttpClientConfig.newBuilder().build());
        final PoolingHttpClientConnectionManager clientConnectionManager = new PoolingHttpClientConnectionManager();
        if (httpClientConfig.getMaxConnectionsPerHost() > 0) {
            clientConnectionManager.setMaxTotal(httpClientConfig.getMaxConnectionsPerHost());
        } else {
            clientConnectionManager.setMaxTotal(Integer.MAX_VALUE);
        }
        clientConnectionManager.setDefaultSocketConfig(SocketConfig.custom()
            .setSoTimeout(httpClientConfig.getSocketTimeoutInMillis() > 0 ? httpClientConfig.getSocketTimeoutInMillis() : 0)
            .build());

        return new HttpClientFactory() {
            @Override
            public HttpClient createClient(final String host, final int port) {
                HttpRoutePlanner rp = new DefaultRoutePlanner(DefaultSchemePortResolver.INSTANCE) {

                    @Override
                    public HttpRoute determineRoute(
                        final HttpHost httpHost,
                        final HttpRequest request,
                        final HttpContext context) throws HttpException {
                        HttpHost target = httpHost != null ? httpHost : new HttpHost(host, port);
                        return super.determineRoute(target, request, context);
                    }
                };
                CloseableHttpClient client = HttpClients.custom()
                    .setConnectionManager(clientConnectionManager)
                    .setRoutePlanner(rp)
                    .build();

                return new ApacheHttpClient441BackedHttpClient(client, httpClientConfig.getCopyOfHeadersForEveryRequest());
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T locateConfig(Collection<HttpClientConfiguration> configurations, Class<? extends T> _class, T defaultConfiguration) {
        for (HttpClientConfiguration configuration : configurations) {
            if (_class.isInstance(configuration)) {
                return (T) configuration;
            }
        }
        return defaultConfiguration;
    }

}
