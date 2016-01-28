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
package com.jivesoftware.os.amza.api.ring;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class RingHost {

    public static final RingHost UNKNOWN_RING_HOST = new RingHost("", "", "unknownRingHost", 0);

    private final String datacenter;
    private final String rack;
    private final String host;
    private final int port;

    public byte[] toBytes() { // TODO convert to lex byte ordering?
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);
        byte[] rackBytes = rack.getBytes(StandardCharsets.UTF_8);
        byte[] datacenterBytes = datacenter.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[1 + 4 + 4 + hostBytes.length + 4 + rackBytes.length + 4 + datacenterBytes.length];
        int i = 0;
        bytes[i] = 1; // version;
        i++;
        UIO.intBytes(port, bytes, i);
        i += 4;
        UIO.intBytes(hostBytes.length, bytes, i);
        i += 4;
        UIO.writeBytes(hostBytes, bytes, i);
        i += hostBytes.length;
        UIO.intBytes(rackBytes.length, bytes, i);
        i += 4;
        UIO.writeBytes(rackBytes, bytes, i);
        i += rackBytes.length;
        UIO.intBytes(datacenterBytes.length, bytes, i);
        i += 4;
        UIO.writeBytes(datacenterBytes, bytes, i);
        return bytes;
    }

    public static RingHost fromBytes(byte[] bytes) throws Exception {
        if (bytes[0] == 0) {
            int port = UIO.bytesInt(bytes, 1);
            String host = new String(bytes, 1 + 4, bytes.length - (1 + 4), StandardCharsets.UTF_8);
            return new RingHost("", "", host, port);
        } else if (bytes[0] == 1) {
            int i = 1;
            int port = UIO.bytesInt(bytes, i);
            i += 4;
            int hostLength = UIO.bytesInt(bytes, i);
            i += 4;
            String host = new String(bytes, i, hostLength, StandardCharsets.UTF_8);
            i += hostLength;
            int rackLength = UIO.bytesInt(bytes, i);
            i += 4;
            String rack = new String(bytes, i, rackLength, StandardCharsets.UTF_8);
            i += rackLength;
            int datacenterLength = UIO.bytesInt(bytes, i);
            i += 4;
            String datacenter = new String(bytes, i, datacenterLength, StandardCharsets.UTF_8);
            return new RingHost(datacenter, rack, host, port);
        }
        return null; // Sorry caller
    }

    @JsonCreator
    public RingHost(@JsonProperty("datacenter") String datacenter,
        @JsonProperty("rack") String rack,
        @JsonProperty("host") String host,
        @JsonProperty("port") int port) {
        this.datacenter = datacenter == null ? "" : datacenter;
        this.rack = rack == null ? "" : rack;
        this.host = host;
        this.port = port;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getRack() {
        return rack;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toCanonicalString() {
        return datacenter + ":" + rack + ":" + host + ":" + port;
    }

    public static RingHost fromCanonicalString(String canonical) {
        String[] split = new String[4];
        int i = 0;
        int fromIndex = 0;
        for (int toIndex = canonical.indexOf(':'); toIndex >= 0; toIndex = canonical.indexOf(':', toIndex + 1)) {
            split[i] = canonical.substring(fromIndex, toIndex);
            fromIndex = toIndex + 1;
            i++;
        }
        split[i] = canonical.substring(fromIndex);
        i++;
        if (i == 2) {
            return new RingHost("", "", split[0], Integer.parseInt(split[1]));
        } else if (i == 4) {
            return new RingHost(split[0], split[1], split[2], Integer.parseInt(split[3]));
        } else {
            throw new IllegalArgumentException("Malformed canonical:" + canonical + " expect: host:port or datacenter:rack:host:port");
        }
    }

    @Override
    public String toString() {
        return "RingHost{" + "datacenter=" + datacenter + ", rack=" + rack + ", host=" + host + ", port=" + port + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 17 * hash + Objects.hashCode(this.host);
        hash = 17 * hash + this.port;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RingHost other = (RingHost) obj;
        if (this.port != other.port) {
            return false;
        }
        if (!Objects.equals(this.host, other.host)) {
            return false;
        }
        return true;
    }

}
