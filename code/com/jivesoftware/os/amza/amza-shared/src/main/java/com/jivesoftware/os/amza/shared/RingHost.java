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
package com.jivesoftware.os.amza.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RingHost implements Comparable<RingHost> {

    private final String host;
    private final int port;

    public byte[] toBytes() { // TODO convert to lex byte ordering?
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[1 + 4 + hostBytes.length];
        bytes[0] = 0; // version;
        UIO.intBytes(port, bytes, 1);
        UIO.bytes(hostBytes, bytes, 1 + 4);
        return bytes;
    }

    public static RingHost fromBytes(byte[] bytes) throws IOException {
        if (bytes[0] == 0) {
            int port = UIO.bytesInt(bytes, 1);
            String host = new String(bytes, 1 + 4, bytes.length - (1 + 4));
            return new RingHost(host, port);
        }
        throw new IOException("Invalid version:" + bytes[0]);
    }

    @JsonCreator
    public RingHost(@JsonProperty("host") String host,
        @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toCanonicalString() {
        return host + ":" + port;
    }

    public static RingHost fromCanonicalString(String canonical) {
        int index = canonical.lastIndexOf(':');
        return new RingHost(canonical.substring(0, index), Integer.parseInt(canonical.substring(index + 1)));
    }

    @Override
    public String toString() {
        return "RingHost{" + "host=" + host + ", port=" + port + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + Objects.hashCode(this.host);
        hash = 79 * hash + this.port;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RingHost other = (RingHost) obj;
        if (!Objects.equals(this.host, other.host)) {
            return false;
        }
        if (this.port != other.port) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(RingHost o) {
        int i = host.compareTo(o.host);
        if (i != 0) {
            return i;
        }
        return Integer.compare(port, o.port);
    }
}
