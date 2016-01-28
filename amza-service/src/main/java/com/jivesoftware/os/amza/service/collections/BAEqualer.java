package com.jivesoftware.os.amza.service.collections;

/**
 *
 * @author jonathan.colt
 */
public class BAEqualer implements Equaler<byte[]> {

    public static final BAEqualer SINGLETON = new BAEqualer();

    @Override
    public boolean equals(byte[] a, byte[] b, int bOffset, int bLength) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }

        if (a.length != bLength) {
            return false;
        }

        for (int i = 0; i < bLength; i++) {
            if (a[i] != b[i + bOffset]) {
                return false;
            }
        }
        return true;
    }

}
