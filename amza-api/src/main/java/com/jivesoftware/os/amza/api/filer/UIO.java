/*
 * UIO.java.java
 *
 * Created on 03-12-2010 11:24:38 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.api.filer;

import java.io.EOFException;
import java.io.IOException;

public class UIO {

    /**
     *
     * @param _from
     * @param _to
     * @param _bufferSize
     * @return
     * @throws Exception
     */
    public static long copy(IReadable _from, IWriteable _to, long _bufferSize) throws Exception {
        long byteCount = _bufferSize;
        if (_bufferSize < 1) {
            byteCount = 1024 * 1024; //1MB
        }
        byte[] chunk = new byte[(int) byteCount];
        int bytesRead = -1;
        long size = 0;
        while ((bytesRead = _from.read(chunk)) > -1) {
            _to.write(chunk, 0, bytesRead);
            size += bytesRead;
        }
        return size;
    }

    /**
     *
     * @param _filer
     * @param bytes
     * @param fieldName
     * @throws IOException
     */
    public static void write(IAppendOnly _filer, byte[] bytes, String fieldName) throws IOException {
        _filer.write(bytes, 0, bytes.length);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeByte(IAppendOnly _filer, byte v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{v}, 0, 1);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeInt(IAppendOnly _filer, int v, String fieldName, byte[] intBuffer) throws IOException {
        intBuffer[0] = (byte) (v >>> 24);
        intBuffer[1] = (byte) (v >>> 16);
        intBuffer[2] = (byte) (v >>> 8);
        intBuffer[3] = (byte) (v);

        _filer.write(intBuffer, 0, 4);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeLong(IAppendOnly _filer, long v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 56),
            (byte) (v >>> 48),
            (byte) (v >>> 40),
            (byte) (v >>> 32),
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        }, 0, 8);
    }

    /**
     *
     * @param _filer
     * @param l
     * @throws IOException
     */
    private static void writeLength(IAppendOnly _filer, int l, byte[] lengthBuffer) throws IOException {
        writeInt(_filer, l, "length", lengthBuffer);
    }

    public static void writeByteArray(IAppendOnly _filer, byte[] array, String fieldName, byte[] lengthBuffer) throws IOException {
        writeByteArray(_filer, array, 0, array == null ? -1 : array.length, fieldName, lengthBuffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param _start
     * @param _len
     * @param fieldName
     * @throws IOException
     */
    public static void writeByteArray(IAppendOnly _filer, byte[] array,
        int _start, int _len, String fieldName, byte[] lengthBuffer) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = _len;
        }
        writeLength(_filer, len, lengthBuffer);
        if (len < 0) {
            return;
        }
        _filer.write(array, _start, len);
    }

    /**
     *
     * @param _filer
     * @return
     * @throws IOException
     */
    public static int readLength(IReadable _filer, byte[] lengthBuffer) throws IOException {
        return readInt(_filer, "length", lengthBuffer);
    }

    public static int readLength(byte[] array, int _offset) throws IOException {
        return UIO.bytesInt(array, _offset);
    }

    /**
     *
     * @param _filer
     * @param array
     * @throws IOException
     */
    public static void read(IReadable _filer, byte[] array) throws IOException {
        readFully(_filer, array, array.length);
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static byte[] readByteArray(IReadable _filer, String fieldName, byte[] lengthBuffer) throws IOException {
        int len = readLength(_filer, lengthBuffer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] array = new byte[len];
        readFully(_filer, array, len);
        return array;
    }

    public static byte[] readByteArray(byte[] bytes, int _offset, String fieldName) throws IOException {
        int len = readLength(bytes, _offset);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] array = new byte[len];
        readFully(bytes, _offset + 4, array, len);
        return array;
    }

    // Reading
    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static boolean readBoolean(IReadable _filer, String fieldName) throws IOException {
        int v = _filer.read();
        if (v < 0) {
            throw new EOFException();
        }
        return (v != 0);
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static byte readByte(IReadable _filer, String fieldName) throws IOException {
        int v = _filer.read();
        if (v < 0) {
            throw new EOFException();
        }
        return (byte) v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readInt(IReadable _filer, String fieldName, byte[] intBuffer) throws IOException {
        readFully(_filer, intBuffer, 4);
        int v = 0;
        v |= (intBuffer[0] & 0xFF);
        v <<= 8;
        v |= (intBuffer[1] & 0xFF);
        v <<= 8;
        v |= (intBuffer[2] & 0xFF);
        v <<= 8;
        v |= (intBuffer[3] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static long readLong(IReadable _filer, String fieldName, byte[] longBuffer) throws IOException {
        readFully(_filer, longBuffer, 8);
        long v = 0;
        v |= (longBuffer[0] & 0xFF);
        v <<= 8;
        v |= (longBuffer[1] & 0xFF);
        v <<= 8;
        v |= (longBuffer[2] & 0xFF);
        v <<= 8;
        v |= (longBuffer[3] & 0xFF);
        v <<= 8;
        v |= (longBuffer[4] & 0xFF);
        v <<= 8;
        v |= (longBuffer[5] & 0xFF);
        v <<= 8;
        v |= (longBuffer[6] & 0xFF);
        v <<= 8;
        v |= (longBuffer[7] & 0xFF);
        return v;
    }

    public static boolean bytesBoolean(byte[] bytes, int _offset) {
        if (bytes == null) {
            return false;
        }
        return bytes[_offset] != 0;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] intBytes(int v) {
        return intBytes(v, new byte[4], 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] intBytes(int v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 24);
        _bytes[_offset + 1] = (byte) (v >>> 16);
        _bytes[_offset + 2] = (byte) (v >>> 8);
        _bytes[_offset + 3] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static int bytesInt(byte[] _bytes) {
        return bytesInt(_bytes, 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] shortBytes(short v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 8);
        _bytes[_offset + 1] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static short bytesShort(byte[] _bytes) {
        return bytesShort(_bytes, 0);
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static int[] bytesInts(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int intsCount = _bytes.length / 4;
        int[] ints = new int[intsCount];
        for (int i = 0; i < intsCount; i++) {
            ints[i] = bytesInt(_bytes, i * 4);
        }
        return ints;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static int bytesInt(byte[] bytes, int _offset) {
        int v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        return v;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static short bytesShort(byte[] bytes, int _offset) {
        short v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _longs
     * @return
     */
    public static byte[] longsBytes(long[] _longs) {
        int len = _longs.length;
        byte[] bytes = new byte[len * 8];
        for (int i = 0; i < len; i++) {
            longBytes(_longs[i], bytes, i * 8);
        }
        return bytes;
    }

    /**
     *
     * @param _v
     * @return
     */
    public static byte[] longBytes(long _v) {
        return longBytes(_v, new byte[8], 0);
    }

    /**
     *
     * @param v
     * @param _bytes
     * @param _offset
     * @return
     */
    public static byte[] longBytes(long v, byte[] _bytes, int _offset) {
        _bytes[_offset + 0] = (byte) (v >>> 56);
        _bytes[_offset + 1] = (byte) (v >>> 48);
        _bytes[_offset + 2] = (byte) (v >>> 40);
        _bytes[_offset + 3] = (byte) (v >>> 32);
        _bytes[_offset + 4] = (byte) (v >>> 24);
        _bytes[_offset + 5] = (byte) (v >>> 16);
        _bytes[_offset + 6] = (byte) (v >>> 8);
        _bytes[_offset + 7] = (byte) v;
        return _bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static long bytesLong(byte[] _bytes) {
        return bytesLong(_bytes, 0);
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static long[] bytesLongs(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int longsCount = _bytes.length / 8;
        long[] longs = new long[longsCount];
        for (int i = 0; i < longsCount; i++) {
            longs[i] = bytesLong(_bytes, i * 8);
        }
        return longs;
    }

    /**
     *
     * @param bytes
     * @param _offset
     * @return
     */
    public static long bytesLong(byte[] bytes, int _offset) {
        if (bytes == null) {
            return 0;
        }
        long v = 0;
        v |= (bytes[_offset + 0] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 1] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 2] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 3] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 4] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 5] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 6] & 0xFF);
        v <<= 8;
        v |= (bytes[_offset + 7] & 0xFF);
        return v;
    }

    /**
     *
     * @param length
     * @param _minPower
     * @return
     */
    public static int chunkPower(long length, int _minPower) {
        if (length == 0) {
            return 0;
        }
        int numberOfTrailingZeros = Long.numberOfLeadingZeros(length - 1);
        return Math.max(_minPower, 64 - numberOfTrailingZeros);
    }

    /**
     *
     * @param _chunkPower
     * @return
     */
    public static long chunkLength(int _chunkPower) {
        return 1L << _chunkPower;
    }

    public static void bytes(byte[] value, byte[] destination, int offset) {
        System.arraycopy(value, 0, destination, offset, value.length);
    }

    private static void readFully(IReadable readable, byte[] into, int length) throws IOException {
        int read = readable.read(into, 0, length);
        if (read != length) {
            throw new IOException("Failed to fully. Only had " + read + " needed " + length);
        }
    }

    private static void readFully(byte[] from, int offset, byte[] into, int length) throws IOException {
        System.arraycopy(from, offset, into, 0, length);
    }
}
