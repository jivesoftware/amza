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
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class RegionName implements Comparable<RegionName>, Serializable {

    private final String ringName;
    private final String regionName;
    private final WALKey minKeyInclusive;
    private final WALKey maxKeyExclusive;

    public byte[] toBytes() throws IOException {
        MemoryFiler memoryFiler = new MemoryFiler();
        UIO.writeByte(memoryFiler, 0, "version");
        UIO.writeString(memoryFiler, ringName, "ringName");
        UIO.writeString(memoryFiler, regionName, "regionName");
        UIO.writeByteArray(memoryFiler, minKeyInclusive == null ?  null : minKeyInclusive.getKey(), "minKeyInclusive");
        UIO.writeByteArray(memoryFiler, maxKeyExclusive == null ?  null : maxKeyExclusive.getKey(), "maxKeyExclusive");
        return memoryFiler.getBytes();
    }

    public static RegionName fromBytes(byte[] bytes) throws IOException {
        MemoryFiler memoryFiler = new MemoryFiler(bytes);
        if (UIO.readByte(memoryFiler, "version") == 0) {
            return new RegionName(
                UIO.readString(memoryFiler, "ringName"),
                UIO.readString(memoryFiler, "ringName"),
                new WALKey(UIO.readByteArray(memoryFiler, "minKeyInclusive")),
                new WALKey(UIO.readByteArray(memoryFiler, "maxKeyExclusive")));
        }
        throw new IOException("Invalid version:" + bytes[0]);
    }

    @JsonCreator
    public RegionName(@JsonProperty("ringName") String ringName,
        @JsonProperty("regionName") String regionName,
        @JsonProperty("minKeyInclusive") WALKey minKeyInclusive,
        @JsonProperty("maxKeyExclusive") WALKey maxKeyExclusive) {
        this.ringName = ringName.toUpperCase();
        this.regionName = regionName;
        this.minKeyInclusive = minKeyInclusive;
        this.maxKeyExclusive = maxKeyExclusive;
    }

    public String getRingName() {
        return ringName;
    }

    public String getRegionName() {
        return regionName;
    }

    public WALKey getMinKeyInclusive() {
        return minKeyInclusive;
    }

    public WALKey getMaxKeyExclusive() {
        return maxKeyExclusive;
    }

    @Override
    public String toString() {
        return "Region{"
            + "name=" + regionName
            + ", ring=" + ringName
            + ", from" + minKeyInclusive
            + ", to" + maxKeyExclusive
            + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 89 * hash + Objects.hashCode(this.ringName);
        hash = 89 * hash + Objects.hashCode(this.regionName);
        hash = 89 * hash + Objects.hashCode(this.minKeyInclusive);
        hash = 89 * hash + Objects.hashCode(this.maxKeyExclusive);
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
        final RegionName other = (RegionName) obj;
        if (!Objects.equals(this.ringName, other.ringName)) {
            return false;
        }
        if (!Objects.equals(this.regionName, other.regionName)) {
            return false;
        }
        if (!Objects.equals(this.minKeyInclusive, other.minKeyInclusive)) {
            return false;
        }
        if (!Objects.equals(this.maxKeyExclusive, other.maxKeyExclusive)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(RegionName o) {
        int i = ringName.compareTo(o.ringName);
        if (i != 0) {
            return i;
        }
        i = regionName.compareTo(o.regionName);
        if (i != 0) {
            return i;
        }
        return i;
    }
}
