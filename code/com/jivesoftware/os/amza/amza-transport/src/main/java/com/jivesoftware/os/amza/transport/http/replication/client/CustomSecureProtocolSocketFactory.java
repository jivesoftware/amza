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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;

public class CustomSecureProtocolSocketFactory implements ProtocolSocketFactory {

    private final SSLSocketFactory sslSocketFactory;

    public CustomSecureProtocolSocketFactory(SSLSocketFactory sslSocketFactory) {
        this.sslSocketFactory = sslSocketFactory;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
            throws IOException {
        return sslSocketFactory.createSocket(host, port, localAddress, localPort);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort, HttpConnectionParams params)
            throws IOException {

        Socket socket = sslSocketFactory.createSocket();

        if (localAddress != null && port > 0) {
            socket.bind(new InetSocketAddress(localAddress, localPort));
        }

        int timeout = params.getSoTimeout();
        if (timeout > 0) {
            socket.setSoTimeout(timeout);
        }

        socket.connect(new InetSocketAddress(host, port), params.getConnectionTimeout());

        return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return sslSocketFactory.createSocket(host, port);
    }
}
