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

public class KeyedTimestampId {

    private final byte[] prefix;
    private final byte[] key;
    private final long timestamp;
    private final boolean tombstoned;

    @JsonCreator
    public KeyedTimestampId(@JsonProperty("prefix") byte[] prefix,
        @JsonProperty("key") byte[] key,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("tombstoned") boolean tombstoned) {
        this.prefix = prefix;
        this.key = key;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    public byte[] getKey() {
        return key;
    }

    public long getTimestampId() {
        return timestamp;
    }

    public boolean getTombstoned() {
        return tombstoned;
    }

    @Override
    public String toString() {
        return "KeyedTimestampId{" +
            "prefix=" + Arrays.toString(prefix) +
            ", key=" + Arrays.toString(key) +
            ", timestamp=" + timestamp +
            ", tombstoned=" + tombstoned +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KeyedTimestampId that = (KeyedTimestampId) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (tombstoned != that.tombstoned) {
            return false;
        }
        if (!Arrays.equals(prefix, that.prefix)) {
            return false;
        }
        return Arrays.equals(key, that.key);

    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + (prefix != null ? Arrays.hashCode(prefix) : 0);
        hash = 17 * hash + (key != null ? Arrays.hashCode(key) : 0);
        hash = 17 * hash + (int) (timestamp ^ (timestamp >>> 32));
        hash = 17 * hash + (tombstoned ? 1 : 0);
        return hash;
    }
}
