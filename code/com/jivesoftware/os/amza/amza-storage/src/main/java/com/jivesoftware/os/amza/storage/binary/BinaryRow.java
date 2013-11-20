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
package com.jivesoftware.os.amza.storage.binary;

import java.io.Serializable;
import java.util.Arrays;

public class BinaryRow implements Serializable {

    public final long transaction;
    public final byte[] key;
    public final long timestamp;
    public final boolean tombstone;
    public final byte[] value;

    public BinaryRow(long transaction, byte[] key, long timestamp, boolean tombstone, byte[] value) {
        this.transaction = transaction;
        this.key = key;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
        this.value = value;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 79 * hash + (int) (this.transaction ^ (this.transaction >>> 32));
        hash = 79 * hash + Arrays.hashCode(this.key);
        hash = 79 * hash + (int) (this.timestamp ^ (this.timestamp >>> 32));
        hash = 79 * hash + (this.tombstone ? 1 : 0);
        hash = 79 * hash + Arrays.hashCode(this.value);
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
        final BinaryRow other = (BinaryRow) obj;
        if (this.transaction != other.transaction) {
            return false;
        }
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        if (this.timestamp != other.timestamp) {
            return false;
        }
        if (this.tombstone != other.tombstone) {
            return false;
        }
        if (!Arrays.equals(this.value, other.value)) {
            return false;
        }
        return true;
    }

}
