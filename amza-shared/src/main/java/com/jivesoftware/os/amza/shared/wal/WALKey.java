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

import com.google.common.primitives.UnsignedBytes;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class WALKey implements Comparable<WALKey> {

    private final byte[] key;
    private transient int hashCode = 0;

    public WALKey(byte[] key) {
        this.key = key;
    }

    /**
     * Please don't mutate this array. Should hand out a copy but trying to make this as fast as possible.
     *
     * @return the key!
     */
    final public byte[] getKey() {
        return key;
    }

    public static byte[] prefixUpperExclusive(byte[] key) {
        byte[] upper = new byte[key.length];
        System.arraycopy(key, 0, upper, 0, key.length);

        // given: [64,72,96,127]
        // want: [64,72,97,-128]
        for (int i = upper.length - 1; i >= 0; i--) {
            if (upper[i] == Byte.MAX_VALUE) {
                upper[i] = Byte.MIN_VALUE;
            } else {
                upper[i]++;
                break;
            }
        }
        return upper;
    }

    @Override
    final public int compareTo(WALKey o) {
        return compare(key, o.key);
    }

    public static int compare(byte[] keyA, byte[] keyB) {
        return UnsignedBytes.lexicographicalComparator().compare(keyA, keyB);
    }

    @Override
    public String toString() {
        return "WALKey{" + "key=" + new String(key, StandardCharsets.US_ASCII) + '}';
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int hash = 3;
            hash = 83 * hash + Arrays.hashCode(this.key);
            hashCode = hash;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final WALKey other = (WALKey) obj;
        if (!Arrays.equals(this.key, other.key)) {
            return false;
        }
        return true;
    }
}
