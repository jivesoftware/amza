package com.jivesoftware.os.amza.service.collections;

/**
 *
 * @author jonathan.colt
 */
public class BAHasher implements Hasher<byte[]> {

    public static final BAHasher SINGLETON = new BAHasher();

    @Override
    public int hashCode(byte[] bytes, int offset, int length) {
        return bytes == null ? 0 : compute(bytes, offset, length);
    }

    private int compute(byte[] a, int offset, int length) {
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }
}
