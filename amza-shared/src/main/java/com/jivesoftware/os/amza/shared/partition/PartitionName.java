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
package com.jivesoftware.os.amza.shared.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.BaseEncoding;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.util.Objects;

public class PartitionName implements Comparable<PartitionName> {

    private final boolean systemPartition;
    private final String ringName;
    private final String name;
    private transient int hash = 0;

    public byte[] toBytes() {
        try {
            HeapFiler memoryFiler = new HeapFiler();
            UIO.writeByte(memoryFiler, 0, "version");
            UIO.writeBoolean(memoryFiler, systemPartition, "systemPartition");
            UIO.writeString(memoryFiler, ringName, "ringName");
            UIO.writeString(memoryFiler, name, "name");
            return memoryFiler.getBytes();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static PartitionName fromBytes(byte[] bytes) {
        try {
            HeapFiler memoryFiler = new HeapFiler(bytes);
            if (UIO.readByte(memoryFiler, "version") == 0) {
                return new PartitionName(
                    UIO.readBoolean(memoryFiler, "systemPartition"),
                    UIO.readString(memoryFiler, "ringName"),
                    UIO.readString(memoryFiler, "name"));
            }
            throw new RuntimeException("Invalid version:" + bytes[0]);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @JsonCreator
    public PartitionName(@JsonProperty("systemPartition") boolean systemPartition,
        @JsonProperty("ringName") String ringName,
        @JsonProperty("name") String name) {
        this.systemPartition = systemPartition;
        this.ringName = ringName.toUpperCase(); // I love this!!! NOT
        this.name = name;
    }

    public String toBase64() throws IOException {
        return BaseEncoding.base64Url().encode(toBytes());
    }

    public static PartitionName fromBase64(String base64) throws IOException {
        return fromBytes(BaseEncoding.base64Url().decode(base64));
    }

    public boolean isSystemPartition() {
        return systemPartition;
    }

    public String getRingName() {
        return ringName;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Partition{"
            + "systemPartition=" + systemPartition
            + ", name=" + name
            + ", ring=" + ringName
            + '}';
    }

    @Override
    public int hashCode() {
        int hash = this.hash;
        if (hash == 0) {
            hash = 7;
            hash = 29 * hash + (this.systemPartition ? 1 : 0);
            hash = 29 * hash + Objects.hashCode(this.ringName);
            hash = 29 * hash + Objects.hashCode(this.name);
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
        final PartitionName other = (PartitionName) obj;
        if (this.systemPartition != other.systemPartition) {
            return false;
        }
        if (!Objects.equals(this.ringName, other.ringName)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(PartitionName o) {
        int i = Boolean.compare(systemPartition, o.systemPartition);
        if (i != 0) {
            return i;
        }
        i = ringName.compareTo(o.ringName);
        if (i != 0) {
            return i;
        }
        i = name.compareTo(o.name);
        if (i != 0) {
            return i;
        }
        return i;
    }
}
