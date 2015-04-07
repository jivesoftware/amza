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
import java.io.Serializable;

public class WALPointer implements Serializable {

    private final long timestamp;
    private final boolean tombstoned;
    private final long fp;

    @JsonCreator
    public WALPointer(
        @JsonProperty("fp") long fp,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("tombstoned") boolean tombstoned) {
        this.fp = fp;
        this.timestamp = timestamp;
        this.tombstoned = tombstoned;
    }

    public long getTimestampId() {
        return timestamp;
    }

    public boolean getTombstoned() {
        return tombstoned;
    }

    public long getFp() {
        return fp;
    }

    @Override
    public String toString() {
        return "WALPointer{" +
            "timestamp=" + timestamp +
            ", tombstoned=" + tombstoned +
            ", fp=" + fp +
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

        WALPointer that = (WALPointer) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (tombstoned != that.tombstoned) {
            return false;
        }
        if (fp != that.fp) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (tombstoned ? 1 : 0);
        result = 31 * result + (int) (fp ^ (fp >>> 32));
        return result;
    }
}
