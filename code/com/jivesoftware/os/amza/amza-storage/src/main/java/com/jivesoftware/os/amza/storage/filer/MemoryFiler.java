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
package com.jivesoftware.os.amza.storage.filer;

import java.io.IOException;

/**
 *
 * All of the methods are intentionally left unsynchronized. Up to caller to do the right thing using the Object returned by lock()
 *
 */
public class MemoryFiler implements IFiler {

    private byte[] bytes = new byte[0];
    private long fp = 0;

    public MemoryFiler() {
    }

    public MemoryFiler(byte[] _bytes) {
        if (_bytes == null) {
            throw new IllegalArgumentException("MemoryFiler _bytes cannot be null.");
        }
        bytes = _bytes;
    }

    public byte[] getBytes() {
        return trim(bytes, (int) fp);
    }

    public byte[] leakBytes() {
        return bytes;
    }

    public void reset() {
        fp = 0;
        bytes = new byte[0];
    }

    @Override
    public Object lock() {
        return this;
    }

    @Override
    public int read() throws IOException {
        if (fp >= bytes.length) {
            return -1;
        }
        int b = bytes[(int) fp] & 0xFF;
        fp++;
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            return 0;
        }
        if (fp > bytes.length) {
            return -1;
        }
        int len = b.length;
        if (bytes.length - (int) fp < len) {
            len = bytes.length - (int) fp;
        }
        if (len == 0) {
            return -1;
        }
        System.arraycopy(bytes, (int) fp, b, 0, len);
        fp += len;
        return len;
    }

    @Override
    public int read(byte b[], int _offset, int _len) throws IOException {
        if (b == null) {
            return 0;
        }
        if (fp > bytes.length) {
            return -1;
        }
        int len = _len;
        if (fp + len > bytes.length) {
            len = (int) (bytes.length - fp);
        }
        System.arraycopy(bytes, (int) fp, b, _offset, len);
        fp += len;
        return len;
    }

    @Override
    public void write(int b) throws IOException {
        if (fp + 1 > bytes.length) {
            bytes = grow(bytes, 1 + (bytes.length * 2));
        }
        bytes[(int) fp] = (byte) b;
        fp++;
    }

    @Override
    public void write(byte _b[]) throws IOException {
        if (_b == null) {
            return;
        }
        int len = _b.length;
        if (fp + len > bytes.length) {
            bytes = grow(bytes, len + (bytes.length * 2));
        }
        System.arraycopy(_b, 0, bytes, (int) fp, len);
        fp += len;
    }

    @Override
    public void write(byte _b[], int _offset, int _len) throws IOException {
        if (_b == null) {
            return;
        }
        int len = _len;
        if (fp + len > bytes.length) {
            bytes = grow(bytes, len + (bytes.length * 2));
        }
        System.arraycopy(_b, _offset, bytes, (int) fp, len);
        fp += len;
    }

    @Override
    public long getFilePointer() throws IOException {
        return fp;
    }

    @Override
    public void seek(long _position) throws IOException {
        fp = _position;
    }

    @Override
    public long skip(long _position) throws IOException {
        fp += _position;
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
    }

    @Override
    public long length() throws IOException {
        return bytes.length;
    }

    @Override
    public void eof() throws IOException {
        bytes = trim(bytes, (int) fp);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void flush() throws IOException {
    }

    private static byte[] trim(byte[] src, int count) {
        byte[] newSrc = new byte[count];
        System.arraycopy(src, 0, newSrc, 0, count);
        return newSrc;
    }

    static final public byte[] grow(byte[] src, int amount) {
        if (src == null) {
            return new byte[amount];
        }
        byte[] newSrc = new byte[src.length + amount];
        System.arraycopy(src, 0, newSrc, 0, src.length);
        return newSrc;
    }
}