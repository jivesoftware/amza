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
package com.jivesoftware.os.amza.shared.filer;

import com.google.common.math.IntMath;
import com.jivesoftware.os.amza.api.filer.IFiler;
import java.io.IOException;

/**
 *
 * All of the methods are intentionally left unsynchronized. Up to caller to do the right thing using the Object returned by lock()
 *
 */
public class HeapFiler implements IFiler {

    private byte[] bytes = new byte[0];
    private long fp = 0;
    private long maxLength = 0;

    public HeapFiler() {
    }

    public HeapFiler(int size) {
        bytes = new byte[size];
        maxLength = 0;
    }

    public HeapFiler(byte[] _bytes) {
        bytes = _bytes;
        maxLength = _bytes.length;
    }

    public HeapFiler createReadOnlyClone() {
        HeapFiler heapFiler = new HeapFiler();
        heapFiler.bytes = bytes;
        heapFiler.maxLength = maxLength;
        return heapFiler;
    }

    public byte[] getBytes() {
        if (maxLength == bytes.length) {
            return bytes;
        } else {
            return trim(bytes, (int) maxLength);
        }
    }

    public byte[] copyUsedBytes() {
        return trim(bytes, (int) maxLength);
    }

    public byte[] leakBytes() {
        return bytes;
    }

    public void reset() {
        fp = 0;
        maxLength = 0;
    }

    @Override
    public Object lock() {
        return this;
    }

    @Override
    public int read() throws IOException {
        if (fp + 1 > maxLength) {
            return -1;
        }
        int b = bytes[(int) fp] & 0xFF;
        fp++;
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int _offset, int _len) throws IOException {
        if (fp > maxLength) {
            return -1;
        }
        int len = _len;
        if (fp + len > maxLength) {
            len = (int) (maxLength - fp);
        }
        System.arraycopy(bytes, (int) fp, b, _offset, len);
        fp += len;
        return len;
    }

    @Override
    public void write(int b) throws IOException {
        if (fp + 1 > bytes.length) {
            bytes = grow(bytes, Math.max((int) ((fp + 1) - bytes.length), bytes.length));
        }
        bytes[(int) fp] = (byte) b;
        fp++;
        maxLength = Math.max(maxLength, fp);
    }

    @Override
    public void write(byte _b[]) throws IOException {
        write(_b, 0, _b.length);
    }

    @Override
    public void write(byte _b[], int _offset, int _len) throws IOException {
        if (_b == null) {
            return;
        }
        int len = _len;
        if (fp + len > bytes.length) {
            bytes = grow(bytes, Math.max((int) ((fp + len) - bytes.length), bytes.length));
        }
        System.arraycopy(_b, _offset, bytes, (int) fp, len);
        fp += len;
        maxLength = Math.max(maxLength, fp);
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public void seek(long _position) throws IOException {
        fp = _position;
        maxLength = Math.max(maxLength, fp);
    }

    @Override
    public long skip(long _position) throws IOException {
        fp += _position;
        maxLength = Math.max(maxLength, fp);
        return fp;
    }

    @Override
    public void setLength(long len) throws IOException {
        if (len < 0) {
            throw new IOException();
        }
        byte[] newBytes = new byte[(int) len];
        System.arraycopy(bytes, 0, newBytes, 0, Math.min(bytes.length, newBytes.length));
        fp = (int) len;
        bytes = newBytes;
        maxLength = len;
    }

    @Override
    public long length() throws IOException {
        return maxLength;
    }

    @Override
    public void eof() throws IOException {
        bytes = trim(bytes, (int) fp);
        maxLength = fp;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void flush(boolean fsync) throws IOException {
    }

    private static byte[] trim(byte[] src, int count) {
        byte[] newSrc = new byte[count];
        System.arraycopy(src, 0, newSrc, 0, count);
        return newSrc;
    }

    static final public byte[] grow(byte[] src, int amount) {
        if (amount < 0) {
            throw new IllegalArgumentException("amount must be greater than zero");
        }
        if (src == null) {
            return new byte[amount];
        }
        byte[] newSrc = new byte[IntMath.checkedAdd(src.length, amount)];
        System.arraycopy(src, 0, newSrc, 0, src.length);
        return newSrc;
    }
}
