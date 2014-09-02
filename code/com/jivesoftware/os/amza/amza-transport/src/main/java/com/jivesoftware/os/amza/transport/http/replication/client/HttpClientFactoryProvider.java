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
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.lang.StringUtils;

public class HttpClientFactoryProvider {

    private static final String HTTPS_PROTOCOL = "https";
    private static final int SSL_PORT = 443;

    public HttpClientFactory createHttpClientFactory(final Collection<HttpClientConfiguration> configurations) {
        return new HttpClientFactory() {
            @Override
            public HttpClient createClient(String host, int port) {

                ApacheHttpClient31BackedHttpClient httpClient = createApacheClient();

                HostConfiguration hostConfiguration = new HostConfiguration();
                configureSsl(hostConfiguration, host, port, httpClient);
                configureProxy(hostConfiguration, httpClient);
                httpClient.setHostConfiguration(hostConfiguration);
                return httpClient;
            }

            private ApacheHttpClient31BackedHttpClient createApacheClient() {
                HttpClientConfig httpClientConfig = locateConfig(HttpClientConfig.class, HttpClientConfig.newBuilder().build());

                HttpConnectionManager connectionManager = createConnectionManager(httpClientConfig);

                org.apache.commons.httpclient.HttpClient client = new org.apache.commons.httpclient.HttpClient(connectionManager);
                client.getParams().setParameter(HttpMethodParams.COOKIE_POLICY, CookiePolicy.RFC_2109);
                client.getParams().setParameter(HttpMethodParams.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
                client.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "UTF-8");
                client.getParams().setBooleanParameter(HttpMethodParams.USE_EXPECT_CONTINUE, false);
                client.getParams().setBooleanParameter(HttpConnectionParams.STALE_CONNECTION_CHECK, true);
                client.getParams().setParameter(HttpConnectionParams.CONNECTION_TIMEOUT,
                        httpClientConfig.getSocketTimeoutInMillis() > 0 ? httpClientConfig.getSocketTimeoutInMillis() : 0);
                client.getParams().setParameter(HttpConnectionParams.SO_TIMEOUT,
                        httpClientConfig.getSocketTimeoutInMillis() > 0 ? httpClientConfig.getSocketTimeoutInMillis() : 0);

                return new ApacheHttpClient31BackedHttpClient(client, httpClientConfig.getCopyOfHeadersForEveryRequest());

            }

            @SuppressWarnings("unchecked")
            private <T> T locateConfig(Class<? extends T> _class, T defaultConfiguration) {
                for (HttpClientConfiguration configuration : configurations) {
                    if (_class.isInstance(configuration)) {
                        return (T) configuration;
                    }
                }
                return defaultConfiguration;
            }

            private boolean hasValidProxyUsernameAndPasswordSettings(HttpClientProxyConfig httpClientProxyConfig) {
                return StringUtils.isNotBlank(httpClientProxyConfig.getProxyUsername()) && StringUtils.isNotBlank(httpClientProxyConfig.getProxyPassword());
            }

            private HttpConnectionManager createConnectionManager(HttpClientConfig config) {
                MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
                if (config.getMaxConnectionsPerHost() > 0) {
                    connectionManager.getParams().setDefaultMaxConnectionsPerHost(config.getMaxConnectionsPerHost());
                } else {
                    connectionManager.getParams().setDefaultMaxConnectionsPerHost(Integer.MAX_VALUE);
                }
                if (config.getMaxConnections() > 0) {
                    connectionManager.getParams().setMaxTotalConnections(config.getMaxConnections());
                }
                return connectionManager;
            }

            private void configureProxy(HostConfiguration hostConfiguration, ApacheHttpClient31BackedHttpClient httpClient) {
                HttpClientProxyConfig httpClientProxyConfig = locateConfig(HttpClientProxyConfig.class, null);
                if (httpClientProxyConfig != null) {
                    hostConfiguration.setProxy(httpClientProxyConfig.getProxyHost(), httpClientProxyConfig.getProxyPort());
                    if (hasValidProxyUsernameAndPasswordSettings(httpClientProxyConfig)) {
                        HttpState state = new HttpState();
                        state.setProxyCredentials(AuthScope.ANY, new UsernamePasswordCredentials(httpClientProxyConfig.getProxyUsername(),
                                httpClientProxyConfig.getProxyPassword()));
                        httpClient.setState(state);
                    }
                }
            }

            private void configureSsl(HostConfiguration hostConfiguration, String host, int port, ApacheHttpClient31BackedHttpClient httpClient)
                    throws IllegalStateException {
                HttpClientSSLConfig httpClientSSLConfig = locateConfig(HttpClientSSLConfig.class, null);
                if (httpClientSSLConfig != null) {
                    Protocol sslProtocol;
                    if (httpClientSSLConfig.getCustomSSLSocketFactory() != null) {
                        sslProtocol = new Protocol(HTTPS_PROTOCOL,
                                new CustomSecureProtocolSocketFactory(httpClientSSLConfig.getCustomSSLSocketFactory()), SSL_PORT);
                    } else {
                        sslProtocol = Protocol.getProtocol(HTTPS_PROTOCOL);
                    }
                    hostConfiguration.setHost(host, port, sslProtocol);
                } else {
                    hostConfiguration.setHost(host, port);
                }
            }
        };
    }
}
