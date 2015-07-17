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
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.shared.filer.HeapFiler;
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.io.IOException;
import java.util.Arrays;

public class PartitionName implements Comparable<PartitionName> {

    private final boolean systemPartition;
    private final byte[] ringName;
    private final byte[] name;
    private transient int hash = 0;

    public byte[] toBytes() {
        try {
            HeapFiler memoryFiler = new HeapFiler();
            UIO.writeByte(memoryFiler, 0, "version");
            UIO.writeBoolean(memoryFiler, systemPartition, "systemPartition");
            UIO.writeByteArray(memoryFiler, ringName, "ringName");
            UIO.writeByteArray(memoryFiler, name, "name");
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
                    UIO.readByteArray(memoryFiler, "ringName"),
                    UIO.readByteArray(memoryFiler, "name"));
            }
            throw new RuntimeException("Invalid version:" + bytes[0]);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @JsonCreator
    public PartitionName(@JsonProperty("systemPartition") boolean systemPartition,
        @JsonProperty("ringName") byte[] ringName,
        @JsonProperty("name") byte[] name) {
        this.systemPartition = systemPartition;
        this.ringName = ringName;
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

    public byte[] getRingName() {
        return ringName;
    }

    public byte[] getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Partition{"
            + "systemPartition=" + systemPartition
            + ", name=" + new String(name)
            + ", ring=" + new String(ringName)
            + '}';
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + (this.systemPartition ? 1 : 0);
        hash = 59 * hash + Arrays.hashCode(this.ringName);
        hash = 59 * hash + Arrays.hashCode(this.name);
        hash = 59 * hash + this.hash;
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
        if (!Arrays.equals(this.ringName, other.ringName)) {
            return false;
        }
        if (!Arrays.equals(this.name, other.name)) {
            return false;
        }
        if (this.hash != other.hash) {
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
        i = UnsignedBytes.lexicographicalComparator().compare(ringName, o.ringName);
        if (i != 0) {
            return i;
        }
        i = UnsignedBytes.lexicographicalComparator().compare(name, o.name);
        if (i != 0) {
            return i;
        }
        return i;
    }
}
