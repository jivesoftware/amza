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

public class WALTimestampId {

    private final long timestamp;
    private final boolean tombstoned;

    @JsonCreator
    public WALTimestampId(@JsonProperty("timestamp") long timestamp,
        @JsonProperty("tombstoned") boolean tombstoned) {
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
    }

    public long getTimestampId() {
        return timestamp;
    }

    public boolean getTombstoned() {
        return tombstoned;
    }

    @Override
    public String toString() {
        return "WALTimestampId{" + "timestamp=" + timestamp + ", tombstoned=" + tombstoned + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WALTimestampId that = (WALTimestampId) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        return tombstoned == that.tombstoned;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (tombstoned ? 1 : 0);
        return result;
    }
}
