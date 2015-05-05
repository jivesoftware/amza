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
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.shared.filer.MemoryFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.util.Objects;

public class RegionName implements Comparable<RegionName> {

    private final boolean systemRegion;
    private final String ringName;
    private final String regionName;
    private transient int hash = 0;

    public byte[] toBytes() throws IOException {
        MemoryFiler memoryFiler = new MemoryFiler();
        UIO.writeByte(memoryFiler, 0, "version");
        UIO.writeBoolean(memoryFiler, systemRegion, "systemRegion");
        UIO.writeString(memoryFiler, ringName, "ringName");
        UIO.writeString(memoryFiler, regionName, "regionName");
        return memoryFiler.getBytes();
    }

    public static RegionName fromBytes(byte[] bytes) throws IOException {
        MemoryFiler memoryFiler = new MemoryFiler(bytes);
        if (UIO.readByte(memoryFiler, "version") == 0) {
            return new RegionName(
                UIO.readBoolean(memoryFiler, "systemRegion"),
                UIO.readString(memoryFiler, "ringName"),
                UIO.readString(memoryFiler, "ringName"));
        }
        throw new IOException("Invalid version:" + bytes[0]);
    }

    public String toBase64() throws IOException {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static RegionName fromBase64(String base64) throws IOException {
        return fromBytes(BaseEncoding.base64Url().decode(base64));
    }

    @JsonCreator
    public RegionName(@JsonProperty("systemRegion") boolean systemRegion,
        @JsonProperty("ringName") String ringName,
        @JsonProperty("regionName") String regionName) {
        this.systemRegion = systemRegion;
        this.ringName = ringName.toUpperCase(); // I love this!!! NOT
        this.regionName = regionName;
    }

    public boolean isSystemRegion() {
        return systemRegion;
    }

    public String getRingName() {
        return ringName;
    }

    public String getRegionName() {
        return regionName;
    }

    @Override
    public String toString() {
        return "Region{"
            + "systemRegion=" + systemRegion
            + ", name=" + regionName
            + ", ring=" + ringName
            + '}';
    }

    @Override
    public int hashCode() {
        int hash = this.hash;
        if (hash == 0) {
            hash = 7;
            hash = 29 * hash + (this.systemRegion ? 1 : 0);
            hash = 29 * hash + Objects.hashCode(this.ringName);
            hash = 29 * hash + Objects.hashCode(this.regionName);
            this.hash = hash;
        }
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
        if (this.systemRegion != other.systemRegion) {
            return false;
        }
        if (!Objects.equals(this.ringName, other.ringName)) {
            return false;
        }
        if (!Objects.equals(this.regionName, other.regionName)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(RegionName o) {
        int i = Boolean.compare(systemRegion, o.systemRegion);
        if (i != 0) {
            return i;
        }
        i = ringName.compareTo(o.ringName);
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
