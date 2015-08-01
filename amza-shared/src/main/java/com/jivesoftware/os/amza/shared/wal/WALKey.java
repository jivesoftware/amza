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
import com.jivesoftware.os.amza.shared.filer.UIO;
import java.util.Arrays;

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

    public static byte[] compose(byte[] prefix, byte[] key) {
        if (key == null) {
            return null;
        }
        int prefixLength = prefix != null ? prefix.length : 0;
        if (prefixLength == 0) {
            byte[] pk = new byte[1 + key.length];
            System.arraycopy(key, 0, pk, 1, key.length);
            return pk;
        } else if (prefixLength <= Byte.MAX_VALUE) {
            byte[] pk = new byte[1 + 1 + prefix.length + key.length];
            pk[0] = 1;
            pk[1] = (byte) prefixLength;
            System.arraycopy(prefix, 0, pk, 1 + 1, prefix.length);
            System.arraycopy(key, 0, pk, 1 + 1 + prefix.length, key.length);
            return pk;
        } else if (prefixLength <= Short.MAX_VALUE) {
            byte[] pk = new byte[1 + 2 + prefix.length + key.length];
            pk[0] = 2;
            UIO.shortBytes((short) prefixLength, pk, 1);
            System.arraycopy(prefix, 0, pk, 1 + 2, prefix.length);
            System.arraycopy(key, 0, pk, 1 + 2 + prefix.length, key.length);
            return pk;
        } else {
            byte[] pk = new byte[1 + 4 + prefix.length + key.length];
            pk[0] = 4;
            UIO.intBytes(prefixLength, pk, 1);
            System.arraycopy(prefix, 0, pk, 1 + 4, prefix.length);
            System.arraycopy(key, 0, pk, 1 + 4 + prefix.length, key.length);
            return pk;
        }
    }

    public static int compare(byte[] composedA, byte[] composedB) {
        return UnsignedBytes.lexicographicalComparator().compare(composedA, composedB);
    }

    public interface RawKeyEntries<R> {

        boolean consume(RawKeyEntryStream<R> rawKeyEntryStream) throws Exception;
    }

    public interface RawKeyEntryStream<R> {

        boolean stream(byte[] rawKey, R entry) throws Exception;
    }

    public interface TxFpRawKeyValues {

        boolean consume(TxFpRawKeyValueStream txFpRawKeyValueStream) throws Exception;
    }

    public interface TxFpRawKeyValueStream {

        boolean stream(long txId, long fp, byte[] rawKey, byte[] value, long valueTimestamp, boolean valueTombstoned) throws Exception;
    }

    public interface WALKeyEntryStream<R> {

        boolean stream(byte[] prefix, byte[] key, R entry) throws Exception;
    }

    public static boolean decompose(TxFpRawKeyValues keyValues, TxFpKeyValueStream stream) throws Exception {
        return keyValues.consume((txId, fp, rawKey, value, valueTimestamp, valueTombstoned) -> {
            byte precision = rawKey[0];
            int prefixLengthInBytes;
            if (precision == 0) {
                prefixLengthInBytes = 0;
            } else if (precision == 1) {
                prefixLengthInBytes = rawKey[1];
            } else if (precision == 2) {
                prefixLengthInBytes = UIO.bytesShort(rawKey, 1);
            } else {
                prefixLengthInBytes = UIO.bytesInt(rawKey, 1);
            }
            byte[] prefix = prefixLengthInBytes > 0 ? new byte[prefixLengthInBytes] : null;
            byte[] key = new byte[rawKey.length - 1 - precision - prefixLengthInBytes];
            if (prefix != null) {
                System.arraycopy(rawKey, 1 + precision, prefix, 0, prefixLengthInBytes);
            }
            System.arraycopy(rawKey, 1 + precision + prefixLengthInBytes, key, 0, key.length);
            return stream.stream(txId, fp, prefix, key, value, valueTimestamp, valueTombstoned, null);
        });
    }

    public static <R> boolean decomposeEntries(RawKeyEntries<R> keyEntries, WALKeyEntryStream<R> stream) throws Exception {
        return keyEntries.consume((rawKey, entry) -> {
            byte precision = rawKey[0];
            int prefixLengthInBytes;
            if (precision == 0) {
                prefixLengthInBytes = 0;
            } else if (precision == 1) {
                prefixLengthInBytes = rawKey[1];
            } else if (precision == 2) {
                prefixLengthInBytes = UIO.bytesShort(rawKey, 1);
            } else {
                prefixLengthInBytes = UIO.bytesInt(rawKey, 1);
            }
            byte[] prefix = prefixLengthInBytes > 0 ? new byte[prefixLengthInBytes] : null;
            byte[] key = new byte[rawKey.length - 1 - precision - prefixLengthInBytes];
            if (prefix != null) {
                System.arraycopy(rawKey, 1 + precision, prefix, 0, prefixLengthInBytes);
            }
            System.arraycopy(rawKey, 1 + precision + prefixLengthInBytes, key, 0, key.length);
            return stream.stream(prefix, key, entry);
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
