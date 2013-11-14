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

import com.google.common.primitives.UnsignedBytes;
import java.io.Serializable;
import java.util.Arrays;

public class RowIndexKey implements Comparable<RowIndexKey>, Serializable {

    private final byte[] key;

    public RowIndexKey(byte[] key) {
        this.key = key;
    }

    /**
     * Please don't mutate this array. Should hand out a copy but trying to make this as fast as possible.
     *
     * @return
     */
    final public byte[] getKey() {
        return key;
    }

    @Override
    final public int compareTo(RowIndexKey o) {
        return UnsignedBytes.lexicographicalComparator().compare(key, o.key);
    }

    final public long getCount() {
        return key.length;
    }

    @Override
    public String toString() {
        return "TableIndexKey{" + "key=" + Arrays.toString(key) + '}';
    }

    // TODO better hashcode?
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + Arrays.hashCode(this.key);
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
        final RowIndexKey other = (RowIndexKey) obj;
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        return true;
    }
}
