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
import java.util.Arrays;

public class KeyedTimestampId {

    private final byte[] key;
    private final long timestamp;
    private final boolean tombstoned;

    @JsonCreator
    public KeyedTimestampId(@JsonProperty("key") byte[] key,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("tombstoned") boolean tombstoned) {
        this.key = key;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
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
        return "KeyedTimestampId{" + "key=" + key + ", timestamp=" + timestamp + ", tombstoned=" + tombstoned + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 17 * hash + Arrays.hashCode(this.key);
        hash = 17 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
        hash = 17 * hash + (this.tombstoned ? 1 : 0);
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
        final KeyedTimestampId other = (KeyedTimestampId) obj;
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        if (this.tombstoned != other.tombstoned) {
            return false;
        }
        return true;
    }

}
