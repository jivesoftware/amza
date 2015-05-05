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

import javax.net.ssl.SSLSocketFactory;

public class HttpClientSSLConfig implements HttpClientConfiguration {

    private final boolean useSSL;
    private final SSLSocketFactory customSSLSocketFactory;

    private HttpClientSSLConfig(boolean useSSL, SSLSocketFactory customSSLSocketFactory) {
        this.useSSL = useSSL;
        this.customSSLSocketFactory = customSSLSocketFactory;
    }

    public boolean isUseSsl() {
        return useSSL;
    }

    public SSLSocketFactory getCustomSSLSocketFactory() {
        return customSSLSocketFactory;
    }

    @Override
    public String toString() {
        return "HttpClientConfig{"
            + ", customSSLSocketFactory=" + customSSLSocketFactory
            + '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    final public static class Builder {

        private boolean useSSL = false;
        private SSLSocketFactory customSSLSocketFactory = null;

        private Builder() {
        }

        public Builder setUseSSL(boolean useSSL) {
            this.useSSL = useSSL;
            return this;
        }

        public Builder setUseSslWithCustomSSLSocketFactory(SSLSocketFactory sslSocketFactory) {
            if (sslSocketFactory == null) {
                throw new IllegalArgumentException("sslSocketFactory cannot be null");
            }
            this.useSSL = true;
            this.customSSLSocketFactory = sslSocketFactory;
            return this;
        }

        public HttpClientSSLConfig build() {
            return new HttpClientSSLConfig(useSSL, customSSLSocketFactory);
        }
    }
}
