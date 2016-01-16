package com.jivesoftware.os.amza.client.http;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import java.util.Arrays;

/**
 *
 * @author jonathan.colt
 */
class QuorumScan {

    private final boolean[] used;
    private final byte[][] prefix;
    private final byte[][] key;
    private final byte[][] value;
    private final long[] timestamp;
    private final long[] version;

    QuorumScan(int streamerCount) {
        this.used = new boolean[streamerCount];
        Arrays.fill(this.used, true);
        this.prefix = new byte[streamerCount][];
        this.key = new byte[streamerCount][];
        this.value = new byte[streamerCount][];
        this.timestamp = new long[streamerCount];
        this.version = new long[streamerCount];
    }

    boolean used(int index) {
        return used[index];
    }

    void fill(int index, byte[] prefix, byte[] key, byte[] value, long timestamp, long version) throws Exception {
        this.used[index] = false;
        this.prefix[index] = prefix;
        this.key[index] = key;
        this.value[index] = value;
        this.timestamp[index] = timestamp;
        this.version[index] = version;
    }

    boolean stream(int wi, KeyValueTimestampStream stream) throws Exception {
        this.used[wi] = true;
        return stream.stream(this.prefix[wi],
            this.key[wi],
            this.value[wi],
            this.timestamp[wi],
            this.version[wi]);
    }

    int findWinningIndex() {
        int wi = -1;
        for (int i = 0; i < used.length; i++) {
            if (used[i]) {
            } else if (wi == -1) {
                wi = i;
            } else {
                wi = winner(wi, i);
            }
        }
        return wi;
    }

    // Smallest lex ordered key with largest timestamp and largest version
    private int winner(int indexA, int indexB) {

        if (used[indexA] && used[indexB]) {
            return -1;
        } else if (used[indexA]) {
            return indexB;
        } else if (used[indexB]) {
            return indexA;
        } else {
            int c = UnsignedBytes.lexicographicalComparator().compare(key[indexA], key[indexB]);
            if (c == 0) {
                c = Long.compare(timestamp[indexA], timestamp[indexB]);
                if (c == 0) {
                    c = Long.compare(version[indexA], version[indexB]);
                    if (c <= 0) {
                        used[indexA] = true;
                        return indexB;
                    } else {
                        used[indexB] = true;
                        return indexA;
                    }
                } else if (c < 0) {
                    used[indexA] = true;
                    return indexB;
                } else {
                    used[indexB] = true;
                    return indexA;
                }
            } else if (c < 0) {
                return indexA;
            } else {
                return indexB;
            }
        }
    }

}
