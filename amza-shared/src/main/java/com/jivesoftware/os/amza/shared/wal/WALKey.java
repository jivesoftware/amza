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

import com.google.common.base.Preconditions;
import com.jivesoftware.os.amza.api.filer.UIO;
import java.util.Arrays;
import java.util.Comparator;

public class WALKey {

    public final byte[] prefix;
    public final byte[] key;

    private transient int hashCode = 0;

    public WALKey(byte[] prefix, byte[] key) {
        this.prefix = prefix;
        this.key = key;
    }

    public byte[] compose() {
        return compose(prefix, key);
    }

    public int sizeOfComposed() {
        return sizeOfComposed(prefix != null ? prefix.length : 0, key.length);
    }

    public static byte[] compose(byte[] prefix, byte[] key) {
        Preconditions.checkNotNull(key, "Key cannot be null");

        int prefixLength = prefix != null ? prefix.length : 0;
        Preconditions.checkArgument(prefixLength <= Short.MAX_VALUE, "Max prefix length is %s", Short.MAX_VALUE);

        byte[] pk = new byte[2 + prefixLength + key.length];
        UIO.shortBytes((short) prefixLength, pk, 0);
        if (prefix != null) {
            System.arraycopy(prefix, 0, pk, 2, prefixLength);
        }
        System.arraycopy(key, 0, pk, 2 + prefixLength, key.length);
        return pk;
    }

    public static int sizeOfComposed(int sizeOfPrefix, int sizeOfKey) {
        return 2 + sizeOfPrefix + sizeOfKey;
    }

    public interface TxFpRawKeyValueEntries<R> {

        boolean consume(TxFpRawKeyValueEntryStream<R> txFpRawKeyValueEntryStream) throws Exception;
    }

    public interface TxFpRawKeyValueEntryStream<R> {

        boolean stream(long txId, long fp, byte[] rawKey, byte[] value, long valueTimestamp, boolean valueTombstoned, R entry) throws Exception;
    }

    public interface TxFpKeyValueEntryStream<R> {

        boolean stream(long txId, long fp, byte[] prefix, byte[] key, byte[] value, long valueTimestamp, boolean valueTombstoned, R entry) throws Exception;
    }

    public static <R> boolean decompose(TxFpRawKeyValueEntries<R> keyEntries, TxFpKeyValueEntryStream<R> stream) throws Exception {
        return keyEntries.consume((txId, fp, rawKey, value, valueTimestamp, valueTombstoned, entry) -> {
            short prefixLengthInBytes = UIO.bytesShort(rawKey);
            byte[] prefix = prefixLengthInBytes > 0 ? new byte[prefixLengthInBytes] : null;
            byte[] key = new byte[rawKey.length - 2 - prefixLengthInBytes];
            if (prefix != null) {
                System.arraycopy(rawKey, 2, prefix, 0, prefixLengthInBytes);
            }
            System.arraycopy(rawKey, 2 + prefixLengthInBytes, key, 0, key.length);
            return stream.stream(txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, entry);
        });
    }

    public static byte[] prefixUpperExclusive(byte[] keyFragment) {
        byte[] upper = new byte[keyFragment.length];
        System.arraycopy(keyFragment, 0, upper, 0, keyFragment.length);

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
    public String toString() {
        return "WALKey{" +
            "key=" + Arrays.toString(key) +
            ", prefix=" + Arrays.toString(prefix) +
            '}';
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int hash = 3;
            hash = 83 * hash + (prefix != null ? Arrays.hashCode(prefix) : 0);
            hash = 83 * hash + (key != null ? Arrays.hashCode(key) : 0);
            hashCode = hash;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WALKey walKey = (WALKey) o;

        if (!Arrays.equals(prefix, walKey.prefix)) {
            return false;
        }
        return Arrays.equals(key, walKey.key);

    }
}
