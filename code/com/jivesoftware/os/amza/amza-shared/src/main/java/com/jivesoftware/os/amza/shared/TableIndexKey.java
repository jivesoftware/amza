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

public class TableIndexKey implements Comparable<TableIndexKey>, Serializable {

    private final byte[] key;
    transient private int hashCode = 0;

    public TableIndexKey(byte[] key) {
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
    final public int compareTo(TableIndexKey o) {
        return UnsignedBytes.lexicographicalComparator().compare(key, o.key);
    }

    final public long getCount() {
        return key.length;
    }

    @Override
    final public int hashCode() {
        if ((key == null) || (key.length == 0)) {
            return 0;
        }

        if (hashCode != 0) {
            return hashCode;
        }

        int hash = 0;
        long randMult = 0x5DEECE66DL;
        long randAdd = 0xBL;
        long randMask = (1L << 48) - 1;
        long seed = key.length;

        for (int i = 0; i < key.length; i++) {
            long x = (seed * randMult + randAdd) & randMask;

            seed = x;
            hash += (key[i] + 128) * x;
        }

        hashCode = hash;

        return hash;
    }

    @Override
    final public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TableIndexKey other = (TableIndexKey) obj;
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        return true;
    }

}
