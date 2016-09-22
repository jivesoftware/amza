package com.jivesoftware.os.amza.client.http;

/**
 *
 */
public class AmzaClientCommitable {
    public final byte[] key;
    public final byte[] value;
    public final long timestamp;

    public AmzaClientCommitable(byte[] key, byte[] value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }
}
