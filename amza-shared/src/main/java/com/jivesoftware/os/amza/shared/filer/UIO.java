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
package com.jivesoftware.os.amza.shared.filer;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

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
     * @param _in
     * @param _bufferSize
     * @return
     * @throws IOException
     */
    public static byte[] toByteArray(InputStream _in, int _bufferSize) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[_bufferSize];
        int bytesRead = -1;
        while ((bytesRead = _in.read(buffer)) > -1) {
            out.write(buffer, 0, bytesRead);
        }
        _in.close();
        return out.toByteArray();
    }

    /**
     *
     * @param _ints
     * @return
     */
    public static byte[] intArrayToByteArray(int[] _ints) {
        int len = _ints.length;
        byte[] bytes = new byte[len * 4]; //peformance hack
        int index = 0;
        for (int i = 0; i < len; i++) {
            int v = _ints[i];
            bytes[index++] = (byte) (v >>> 24);
            bytes[index++] = (byte) (v >>> 16);
            bytes[index++] = (byte) (v >>> 8);
            bytes[index++] = (byte) v;
        }
        return bytes;
    }

    // Writing
    /**
     *
     * @param _filer
     * @param _line
     * @throws IOException
     */
    public static void writeLine(IWriteable _filer, String _line) throws IOException {
        _filer.write((_line + "\n").getBytes());
    }

    /**
     *
     * @param _filer
     * @param b
     * @param fieldName
     * @throws IOException
     */
    public static void write(IWriteable _filer, int b, String fieldName) throws IOException {
        _filer.write(b);
    }

    /**
     *
     * @param _filer
     * @param bytes
     * @param fieldName
     * @throws IOException
     */
    public static void write(IWriteable _filer, byte[] bytes,
        String fieldName) throws IOException {
        _filer.write(bytes);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeBoolean(IWriteable _filer, boolean v,
        String fieldName) throws IOException {
        _filer.write(v ? 1 : 0);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeByte(IWriteable _filer, int v,
        String fieldName) throws IOException {
        _filer.write(v);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeShort(IWriteable _filer, int v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeChar(IWriteable _filer, int v,
        String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeInt(IWriteable _filer, int v, String fieldName) throws IOException {
        _filer.write(new byte[]{
            (byte) (v >>> 24),
            (byte) (v >>> 16),
            (byte) (v >>> 8),
            (byte) v
        });
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeLong(IWriteable _filer, long v,
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
        });
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] intsToBytes(int[] v) {
        byte[] result = new byte[4 * v.length];
        for (int i = 0; i < v.length; i++) {
            int j = i * 4;
            result[j + 0] = (byte) (v[i] >>> 24);
            result[j + 1] = (byte) (v[i] >>> 16);
            result[j + 2] = (byte) (v[i] >>> 8);
            result[j + 3] = (byte) v[i];
        }
        return result;
    }

    /**
     *
     * @param v
     * @return
     */
    public static int[] bytesToInts(byte[] v) {
        int[] result = new int[(v.length + 3) / 4];
        for (int i = 0; i < result.length; i++) {
            int j = i * 4;
            int k = 0;
            k |= (v[j + 0] & 0xFF);
            k <<= 8;
            k |= (v[j + 1] & 0xFF);
            k <<= 8;
            k |= (v[j + 2] & 0xFF);
            k <<= 8;
            k |= (v[j + 3] & 0xFF);
            result[i] = k;
        }
        return result;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] charBytes(char v) {
        return new byte[]{
            (byte) (v >>> 8),
            (byte) v
        };
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeFloat(IWriteable _filer, float v,
        String fieldName) throws IOException {
        writeInt(_filer, Float.floatToIntBits(v), fieldName);
    }

    /**
     *
     * @param _filer
     * @param v
     * @param fieldName
     * @throws IOException
     */
    public static void writeDouble(IWriteable _filer, double v,
        String fieldName) throws IOException {
        writeLong(_filer, Double.doubleToLongBits(v), fieldName);
    }

    /**
     *
     * @param _filer
     * @param l
     * @throws IOException
     */
    public static void writeLength(IWriteable _filer, int l) throws IOException {
        writeInt(_filer, l, "length");
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeBooleanArray(IWriteable _filer,
        boolean[] array, String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            _filer.write(array[i] ? 1 : 0);
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @throws IOException
     */
    public static void write(IWriteable _filer, byte[] array) throws IOException {
        _filer.write(array);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeByteArray(IWriteable _filer, byte[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        _filer.write(array);
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
    public static void writeByteArray(IWriteable _filer, byte[] array,
        int _start, int _len, String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = _len;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        _filer.write(array, _start, len);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeCharArray(IWriteable _filer, char[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        int index = 0;
        byte[] buffer = new byte[len * 2]; //peformance hack
        for (int i = 0; i < len; i++) {
            int v = array[i];
            buffer[index++] = (byte) (v >>> 8);
            buffer[index++] = (byte) v;
        }
        _filer.write(buffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeShortArray(IWriteable _filer, short[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        int index = 0;
        byte[] buffer = new byte[len * 2]; //peformance hack
        for (int i = 0; i < len; i++) {
            int v = array[i];
            buffer[index++] = (byte) (v >>> 8);
            buffer[index++] = (byte) v;
        }
        _filer.write(buffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeIntArray(IWriteable _filer, int[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        int index = 0;
        byte[] buffer = new byte[len * 4]; //peformance hack
        for (int i = 0; i < len; i++) {
            int v = array[i];
            buffer[index++] = (byte) (v >>> 24);
            buffer[index++] = (byte) (v >>> 16);
            buffer[index++] = (byte) (v >>> 8);
            buffer[index++] = (byte) v;
        }
        _filer.write(buffer);
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeStringArray(IWriteable _filer, Object[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            String s = null;
            if (array[i] instanceof String) {
                s = (String) array[i];
            } else if (array[i] != null) {
                s = array[i].toString();
            }
            if (s == null) {
                s = "";
            }
            writeCharArray(_filer, s.toCharArray(), fieldName);
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeLongArray(IWriteable _filer, long[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            long v = array[i];
            _filer.write(new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
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
    public static void writeLongArray(IWriteable _filer, long[] array,
        int _start, int _len, String fieldName) throws IOException {
        writeLength(_filer, _len);
        if (_len < 0) {
            return;
        }
        for (int i = _start; i < _start + _len; i++) {
            long v = array[i];
            _filer.write(new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeFloatArray(IWriteable _filer, float[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            int v = Float.floatToIntBits(array[i]);
            _filer.write(new byte[]{
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param array
     * @param fieldName
     * @throws IOException
     */
    public static void writeDoubleArray(IWriteable _filer, double[] array,
        String fieldName) throws IOException {
        int len;
        if (array == null) {
            len = -1;
        } else {
            len = array.length;
        }
        writeLength(_filer, len);
        if (len < 0) {
            return;
        }
        for (int i = 0; i < len; i++) {
            long v = Double.doubleToLongBits(array[i]);
            _filer.write(new byte[]{
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
            });
        }
    }

    /**
     *
     * @param _filer
     * @param s
     * @param fieldName
     * @throws IOException
     */
    public static void writeString(IWriteable _filer, String s,
        String fieldName) throws IOException {
        if (s == null) {
            s = "";
        }
        writeByteArray(_filer, s.getBytes(StandardCharsets.UTF_8), fieldName);
    }

    /**
     *
     * @param _filer
     * @param _tag
     * @throws Exception
     */
    public static void readBegin(IReadable _filer, String _tag) throws Exception {
    }

    /**
     *
     * @param _filer
     * @param _tag
     * @throws Exception
     */
    public static void readEnd(IReadable _filer, String _tag) throws Exception {
    }

    /**
     *
     * @param _filer
     * @return
     * @throws IOException
     */
    public static int readLength(IReadable _filer) throws IOException {
        return readInt(_filer, "length");
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static boolean[] readBooleanArray(IReadable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new boolean[0];
        }
        boolean[] array = new boolean[len];
        byte[] bytes = new byte[len];
        readFully(_filer, bytes, len);
        for (int i = 0; i < len; i++) {
            array[i] = (bytes[i] != 0);
        }
        return array;
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
    public static byte[] readByteArray(IReadable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
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

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static char[] readCharArray(IReadable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new char[0];
        }
        char[] array = new char[len];
        byte[] bytes = new byte[2 * len];
        readFully(_filer, bytes, 2 * len);
        for (int i = 0; i < len; i++) {
            int j = i * 2;
            char v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static short[] readShortArray(IReadable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new short[0];
        }
        short[] array = new short[len];
        byte[] bytes = new byte[2 * len];
        readFully(_filer, bytes, 2 * len);
        for (int i = 0; i < len; i++) {
            int j = i * 2;
            short v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int[] readIntArray(IReadable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new int[0];
        }
        int[] array = new int[len];
        byte[] bytes = new byte[4 * len];
        readFully(_filer, bytes, 4 * len);
        for (int i = 0; i < len; i++) {
            int j = i * 4;
            int v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static String[] readStringArray(IReadable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new String[0];
        }
        String[] array = new String[len];
        for (int i = 0; i < len; i++) {
            array[i] = readString(_filer, fieldName);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static long[] readLongArray(IReadable _filer, String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new long[0];
        }
        long[] array = new long[len];
        byte[] bytes = new byte[8 * len];
        readFully(_filer, bytes, 8 * len);
        for (int i = 0; i < len; i++) {
            int j = i * 8;
            long v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 4] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 5] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 6] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 7] & 0xFF);
            array[i] = v;
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static float[] readFloatArray(IReadable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new float[0];
        }
        float[] array = new float[len];
        byte[] bytes = new byte[4 * len];
        readFully(_filer, bytes, 4 * len);
        for (int i = 0; i < len; i++) {
            int j = i * 4;
            int v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            array[i] = Float.intBitsToFloat(v);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static double[] readDoubleArray(IReadable _filer,
        String fieldName) throws IOException {
        int len = readLength(_filer);
        if (len < 0) {
            return null;
        }
        if (len == 0) {
            return new double[0];
        }
        double[] array = new double[len];
        byte[] bytes = new byte[8 * len];
        readFully(_filer, bytes, 8 * len);
        for (int i = 0; i < len; i++) {
            int j = i * 8;
            long v = 0;
            v |= (bytes[j + 0] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 1] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 2] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 3] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 4] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 5] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 6] & 0xFF);
            v <<= 8;
            v |= (bytes[j + 7] & 0xFF);
            array[i] = Double.longBitsToDouble(v);
        }
        return array;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static String readText(IReadable _filer, String fieldName) throws Exception {
        return new String(readCharArray(_filer, fieldName));
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static String readString(IReadable _filer, String fieldName) throws IOException {
        return new String(readByteArray(_filer, fieldName), StandardCharsets.UTF_8);
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
    public static int readUnsignedByte(IReadable _filer, String fieldName) throws IOException {
        int v = _filer.read();
        if (v < 0) {
            throw new EOFException();
        }
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static short readShort(IReadable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[2];
        readFully(_filer, bytes, 2);
        short v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readUnsignedShort(IReadable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[2];
        readFully(_filer, bytes, 2);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static char readChar(IReadable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[2];
        readFully(_filer, bytes, 2);
        char v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static char bytesChar(byte[] bytes) {
        char v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static int readInt(IReadable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[4];
        readFully(_filer, bytes, 4);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws IOException
     */
    public static long readLong(IReadable _filer, String fieldName) throws IOException {
        byte[] bytes = new byte[8];
        readFully(_filer, bytes, 8);
        long v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        v <<= 8;
        v |= (bytes[4] & 0xFF);
        v <<= 8;
        v |= (bytes[5] & 0xFF);
        v <<= 8;
        v |= (bytes[6] & 0xFF);
        v <<= 8;
        v |= (bytes[7] & 0xFF);
        return v;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static float readFloat(IReadable _filer, String fieldName) throws Exception {
        byte[] bytes = new byte[4];
        readFully(_filer, bytes, 4);
        int v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        return Float.intBitsToFloat(v);
    }

    /**
     *
     * @param chars
     * @return
     */
    public static byte[] charsBytes(char[] chars) {
        byte[] bytes = new byte[chars.length * 2];
        for (int i = 0; i < chars.length; i++) {
            char v = chars[i];
            bytes[2 * i] = (byte) (v >>> 8);
            bytes[2 * i + 1] = (byte) v;
        }
        return bytes;
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static char[] bytesChars(byte[] bytes) {
        char[] chars = new char[bytes.length / 2];
        for (int i = 0; i < chars.length; i++) {
            char v = 0;
            v |= (bytes[2 * i] & 0xFF);
            v <<= 8;
            v |= (bytes[2 * i + 1] & 0xFF);
            chars[i] = v;
        }
        return chars;
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static double readDouble(IReadable _filer, String fieldName) throws Exception {
        byte[] bytes = new byte[8];
        readFully(_filer, bytes, 8);
        long v = 0;
        v |= (bytes[0] & 0xFF);
        v <<= 8;
        v |= (bytes[1] & 0xFF);
        v <<= 8;
        v |= (bytes[2] & 0xFF);
        v <<= 8;
        v |= (bytes[3] & 0xFF);
        v <<= 8;
        v |= (bytes[4] & 0xFF);
        v <<= 8;
        v |= (bytes[5] & 0xFF);
        v <<= 8;
        v |= (bytes[6] & 0xFF);
        v <<= 8;
        v |= (bytes[7] & 0xFF);
        return Double.longBitsToDouble(v);
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static double byteDouble(byte[] bytes) {
        return Double.longBitsToDouble(bytesLong(bytes));
    }

    /**
     *
     * @param _filer
     * @param fieldName
     * @return
     * @throws Exception
     */
    public static String readLine(IFiler _filer, String fieldName) throws Exception {
        StringBuilder input = new StringBuilder();
        int c = -1;
        boolean eol = false;

        while (!eol) {
            switch (c = _filer.read()) {
                case -1:
                case '\n':
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = _filer.getFilePointer();
                    if ((_filer.read()) != '\n') {
                        _filer.seek(cur);
                    }
                    break;
                default:
                    input.append((char) c);
                    break;
            }
        }

        if ((c == -1) && (input.length() == 0)) {
            return null;
        }
        return input.toString();
    }

    /**
     *
     * @param _bytes
     * @param _offset
     * @param _longs
     * @return
     */
    static public boolean unpackLongs(byte[] _bytes, int _offset, long[] _longs) {
        if (_bytes == null || _bytes.length == 0) {
            return false;
        }
        if (_longs == null || _longs.length == 0) {
            return false;
        }
        int longsCount = _longs.length;
        if (_offset + (longsCount * 8) > _bytes.length) {
            return false;
        }
        for (int i = 0; i < longsCount; i++) {
            _longs[i] = UIO.bytesLong(_bytes, _offset + (i * 8));
        }
        return true;
    }

    /**
     *
     * @param _a
     * @return
     */
    static public byte[] packLongs(long _a) {
        return packLongs(new long[]{_a});
    }

    /**
     *
     * @param _a
     * @param _b
     * @return
     */
    static public byte[] packLongs(long _a, long _b) {
        return packLongs(new long[]{_a, _b});
    }

    /**
     *
     * @param _a
     * @param _b
     * @param _c
     * @return
     */
    static public byte[] packLongs(long _a, long _b, long _c) {
        return packLongs(new long[]{_a, _b, _c});
    }

    /**
     *
     * @param _a
     * @param _b
     * @param _c
     * @param _d
     * @return
     */
    static public byte[] packLongs(long _a, long _b, long _c, long _d) {
        return packLongs(new long[]{_a, _b, _c, _d});
    }

    /**
     *
     * @param _a
     * @param _b
     * @param _c
     * @param _d
     * @param _e
     * @return
     */
    static public byte[] packLongs(long _a, long _b, long _c, long _d, long _e) {
        return packLongs(new long[]{_a, _b, _c, _d, _e});
    }

    /**
     *
     * @param _batch
     * @return
     */
    static public byte[] packLongs(long[] _batch) {
        if (_batch == null || _batch.length == 0) {
            return null;
        }
        byte[] bytes = new byte[_batch.length * 8];
        for (int i = 0; i < _batch.length; i++) {
            UIO.longBytes(_batch[i], bytes, i * 8);
        }
        return bytes;
    }

    /**
     *
     * @param _bytes
     * @return
     */
    static public long[] unpackLongs(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int longsCount = _bytes.length / 8;
        long[] longs = new long[longsCount];
        for (int i = 0; i < longsCount; i++) {
            longs[i] = UIO.bytesLong(_bytes, i * 8);
        }
        return longs;
    }

    /**
     *
     * @param _bytes
     * @param _lengths
     * @return
     */
    public static Object[] split(byte[] _bytes, int[] _lengths) {
        Object[] splits = new Object[_lengths.length];
        int bp = 0;
        for (int i = 0; i < _lengths.length; i++) {
            byte[] split = new byte[_lengths[i]];
            System.arraycopy(_bytes, bp, split, 0, _lengths[i]);
            splits[i] = split;
        }
        return splits;
    }

    /**
     *
     * @param _ints
     * @return
     */
    public static byte[] intsBytes(int[] _ints) {
        int len = _ints.length;
        byte[] bytes = new byte[len * 4];
        for (int i = 0; i < len; i++) {
            intBytes(_ints[i], bytes, i * 4);
        }
        return bytes;
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
     * @param _floats
     * @return
     */
    public static byte[] floatsBytes(float[] _floats) {
        int len = _floats.length;
        byte[] bytes = new byte[len * 4]; //peformance hack
        for (int i = 0; i < len; i++) {
            intBytes(Float.floatToIntBits(_floats[i]), bytes, i * 4);
        }
        return bytes;
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] floatBytes(float v) {
        return intBytes(Float.floatToIntBits(v));
    }

    /**
     *
     * @param bytes
     * @return
     */
    public static float bytesFloat(byte[] bytes) {
        return Float.intBitsToFloat(bytesInt(bytes));
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static float[] bytesFloats(byte[] _bytes) {
        if (_bytes == null || _bytes.length == 0) {
            return null;
        }
        int floatsCount = _bytes.length / 4;
        float[] floats = new float[floatsCount];
        for (int i = 0; i < floatsCount; i++) {
            floats[i] = bytesFloat(_bytes, i * 4);
        }
        return floats;
    }

    /**
     *
     * @param _bytes
     * @param _offset
     * @return
     */
    public static float bytesFloat(byte[] _bytes, int _offset) {
        return Float.intBitsToFloat(bytesInt(_bytes, _offset));
    }

    /**
     *
     * @param _batch
     * @return
     */
    static public byte[] doublesBytes(double[] _batch) {
        long[] batch = new long[_batch.length];
        for (int i = 0; i < batch.length; i++) {
            batch[i] = Double.doubleToLongBits(_batch[i]);
        }
        return packLongs(batch);
    }

    /**
     *
     * @param v
     * @return
     */
    public static byte[] doubleBytes(double v) {
        return longBytes(Double.doubleToLongBits(v));
    }

    /**
     *
     * @param _bytes
     * @return
     */
    public static double bytesDouble(byte[] _bytes) {
        return Double.longBitsToDouble(bytesLong(_bytes));
    }

    /**
     *
     * @param _bytes
     * @return
     */
    static public double[] bytesDoubles(byte[] _bytes) {
        long[] _batch = unpackLongs(_bytes);
        double[] batch = new double[_batch.length];
        for (int i = 0; i < batch.length; i++) {
            batch[i] = Double.longBitsToDouble(_batch[i]);
        }
        return batch;
    }

    /**
     *
     * @param _bytes
     * @param _offset
     * @return
     */
    public static double bytesDouble(byte[] _bytes, int _offset) {
        return Double.longBitsToDouble(bytesLong(_bytes, _offset));
    }

    /**
     *
     * @param _length
     * @param _minPower
     * @return
     */
    public static long chunkPower(long _length, int _minPower) {
        for (long i = _minPower; i < 65; i++) { // 2^64 == long so why go anyfuther
            if (_length < Math.pow(2, i)) {
                return i;
            }
        }
        return 64;
    }

    /**
     *
     * @param _chunkPower
     * @return
     */
    public static long chunkLength(long _chunkPower) {
        return (long) Math.pow(2, _chunkPower);
    }

    public static void bytes(byte[] value, byte[] destination, int offset) {
        System.arraycopy(value, 0, destination, offset, value.length);
    }

    private static void readFully(IReadable readable, byte[] into, int length) throws IOException {
        int read = readable.read(into);
        if (read != length) {
            throw new IOException("Failed to fully. Only had " + read + " needed " + length);
        }
    }
}
