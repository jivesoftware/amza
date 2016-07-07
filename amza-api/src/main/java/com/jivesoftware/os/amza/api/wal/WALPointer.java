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
package com.jivesoftware.os.amza.api.wal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;

public class WALPointer {

    private final long timestamp;
    private final boolean tombstoned;
    private final long version;
    private final long fp;
    private final boolean hasValue;
    private final byte[] value;

    @JsonCreator
    public WALPointer(
        @JsonProperty("fp") long fp,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("tombstoned") boolean tombstoned,
        @JsonProperty("version") long version,
        @JsonProperty("hasValue") boolean hasValue,
        @JsonProperty("value") byte[] value) {
        this.fp = fp;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
        this.version = version;
        this.hasValue = hasValue;
        this.value = value;
    }

    public long getTimestampId() {
        return timestamp;
    }

    public boolean getTombstoned() {
        return tombstoned;
    }

    public long getVersion() {
        return version;
    }

    public long getFp() {
        return fp;
    }

    public boolean getHasValue() {
        return hasValue;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "WALPointer{" +
            "timestamp=" + timestamp +
            ", tombstoned=" + tombstoned +
            ", version=" + version +
            ", fp=" + fp +
            ", hasValue=" + hasValue +
            ", value=" + Arrays.toString(value) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }
}
