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
package com.jivesoftware.os.amza.shared.wal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.amza.api.stream.RowType;
import java.util.Arrays;

public class WALValue {

    private final RowType rowType;
    private final byte[] value;
    private final long timestamp;
    private final boolean tombstoned;
    private final long version;

    @JsonCreator
    public WALValue(
        @JsonProperty("rowType") RowType rowType,
        @JsonProperty("value") byte[] value,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("tombstoned") boolean tombstoned,
        @JsonProperty("version") long version) {
        this.rowType = rowType;
        this.timestamp = timestamp;
        this.value = value;
        this.tombstoned = tombstoned;
        this.version = version;
    }

    public RowType getRowType() {
        return rowType;
    }

    public byte[] getValue() {
        return value;
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

    @Override
    public String toString() {
        return "WALValue{"
            + "rowType=" + rowType
            + ", timestamp=" + timestamp
            + ", tombstoned=" + tombstoned
            + ", version=" + version
            + ", value=" + (value != null ? Arrays.toString(value) : "null") + '}';
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
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
        final WALValue other = (WALValue) obj;
        if (this.timestamp != other.timestamp) {
            return false;
        }
        if (this.tombstoned != other.tombstoned) {
            return false;
        }
        if (this.version != other.version) {
            return false;
        }
        if (this.rowType != other.rowType) {
            return false;
        }
        if (!Arrays.equals(this.value, other.value)) {
            return false;
        }
        return true;
    }

}
