package com.jivesoftware.os.amza.lsm;

import com.jivesoftware.os.amza.api.filer.UIO;
import com.jivesoftware.os.amza.lsm.api.MergeRawEntry;

/**
 *
 * @author jonathan.colt
 */
public class SimpleRawEntry implements MergeRawEntry {

    static String toString(byte[] rawEntry) {
        return "key:" + key(rawEntry) + " value:" + value(rawEntry);
    }

    @Override
    public byte[] merge(byte[] current, byte[] adding) {
        return value(current) > value(adding) ? current : adding;
    }

    public static long key(byte[] rawEntry) {
        return UIO.bytesLong(rawEntry, 4);
    }

    public static long value(byte[] rawEntry) {
        return UIO.bytesLong(rawEntry, 4 + 8);
    }

    public static byte[] rawEntry(long key, long value) {
        byte[] rawEntry = new byte[4 + 8 + 8];
        UIO.intBytes(8, rawEntry, 0);
        UIO.longBytes(key, rawEntry, 4);
        UIO.longBytes(value, rawEntry, 4 + 8);
        return rawEntry;
    }
}
